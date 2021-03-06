#!/usr/bin/env python

import copy
import csv
import hashlib
import logging
from optparse import OptionParser
import re
import simplejson
import sqlite3
import threading
import time
from threading import Thread
from uuid import uuid4

# CouchDB imports
import couchdb

DB_FILE = 'bulk.sqlite3.db'
CACHE_TABLE = 'cache'
TMP_TABLE = 'tmp'

def setupdb():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    
    # Creates the cache table:
    c.execute('create table if not exists ' + CACHE_TABLE + 
              '(recguid text, ' +
              'rechash text, ' +
              'recjson text, ' +
              'docid text, ' +
              'docrev text)')
    
    # Creates the temporary table:
    c.execute('create table if not exists ' + TMP_TABLE + 
              '(recguid text, ' +
              'rechash text, ' +
              'recjson text)')
    
    # Clears all records from the temporary table:
    c.execute('delete from %s' % TMP_TABLE)

    c.close()
    return conn

def tmprecgen(seq):
    for row in seq:
        recguid = row['occurrenceID']

        # Creates record hash:
        cols = row.keys()
        cols.sort()
        fields = [row[x].strip() for x in cols]
        line = reduce(lambda x,y: '%s%s' % (x, y), fields)

        rechash = hashlib.sha224(line).hexdigest()
        recjson = simplejson.dumps(row)    

        yield(recguid, rechash, recjson)

class TmpTable(object):
    def __init__(self, conn):
        self.conn = conn
        self.table = 'tmp'
        self.insertsql = 'insert into %s values (?, ?, ?)' % self.table

    def _rowgenerator(self, rows):
        for row in rows:
            recguid = row['occurrenceID']
            cols = row.keys()
            cols.sort()
            fields = [row[x].strip() for x in cols]
            line = reduce(lambda x,y: '%s%s' % (x, y), fields)
            rechash = hashlib.sha224(line).hexdigest()
            recjson = simplejson.dumps(row)
            yield (recguid, rechash, recjson)
        
    def _insertchunk(self, rows, cursor):
        logging.info('%s prepared' % self.totalcount)
        cursor.executemany(self.insertsql, self._rowgenerator(rows))
        self.conn.commit()
        
    def insert(self, csvfile, chunksize):
        logging.info('Inserting %s to tmp table.' % csvfile)

        rows = []
        count = 0
        self.totalcount = 0
        chunkcount = 0
        cursor = self.conn.cursor()
        reader = csv.DictReader(open(csvfile, 'r'), skipinitialspace=True)

        for row in reader:
            if count >= chunksize:
                self.totalcount += count
                self._insertchunk(rows, cursor)
                count = 0
                rows = []        
                chunkcount += 1
            rows.append(row)
            count += 1

        if count > 0:
            self.totalcount += count
            self._insertchunk(rows, cursor)

        logging.info('%s rows inserted to tmp table' % self.totalcount)
 

class NewRecords(object):

    def __init__(self, conn, couch):
        self.conn = conn
        self.couch = couch
        self.insertsql = 'insert into %s values (?, ?, ?, ?, ?)' % CACHE_TABLE            
        self.updatesql = 'update %s set docrev=? where docid=?' % CACHE_TABLE
        self.deltasql = "SELECT * FROM %s LEFT OUTER JOIN %s USING (recguid) WHERE %s.recguid is null"
        self.totalcount = 0
    
    def _insertchunk(self, cursor, recs, docs):
        logging.info('%s inserted' % self.totalcount)
        #cursor.executemany(self.insertsql, recs)
        #self.conn.commit()
        bulk = []
        index = 0
        for doc in self.couch.update(docs):
            bulk.append((recs[index]) + (doc[2],))
            index += 1
        #recs = [(doc[2], doc[1]) for doc in self.couch.update(docs)]
        cursor.executemany(self.insertsql, bulk)
        self.conn.commit()
        
    def execute(self, chunksize):
        logging.info("Checking for new records")

        cursor = self.conn.cursor()
        newrecs = cursor.execute(self.deltasql % (TMP_TABLE, CACHE_TABLE, CACHE_TABLE))
        docs = []
        recs = []
        count = 0
        self.totalcount = 0

        for row in newrecs.fetchall():
            if count >= chunksize:
                self.totalcount += count
                self._insertchunk(cursor, recs, docs)
                count = 0
                recs = []
                docs = []
            count += 1
            recguid = row[0]
            rechash = row[1]
            docid = uuid4().hex
            recjson = row[2]
            doc = simplejson.loads(recjson)
            doc['_id'] = docid
            docs.append(doc)
            recs.append((recguid, rechash, recjson, docid))

        if count > 0:
            self.totalcount += count
            self._insertchunk(cursor, recs, docs)

        self.conn.commit()
        logging.info('INSERT: %s records inserted' % self.totalcount)
    

class UpdatedRecords(object):

    def __init__(self, conn, couch):
        self.conn = conn
        self.couch = couch
        self.updatedocrevsql = 'update %s set docrev=? where docid=?' % CACHE_TABLE
        self.updatesql = 'update %s set rechash=?, recjson=? where docid=?' % CACHE_TABLE
        self.deltasql = 'SELECT c.recguid, t.rechash, c.recjson, c.docid, c.docrev FROM %s as t, %s as c WHERE t.recguid = c.recguid AND t.rechash <> c.rechash' % (TMP_TABLE, CACHE_TABLE)

    def _updatechunk(self, cursor, recs, docs):
        """Bulk inserts docs to couchdb and updates cache table doc revision."""
        logging.info('%s updated' % self.totalcount)
        cursor.executemany(self.updatesql, recs)
        self.conn.commit()
        updates = [(doc[2], doc[1]) for doc in self.couch.update(docs)]
        cursor.executemany(self.updatedocrevsql, updates)
        self.conn.commit()

    def execute(self, chunksize):
        logging.info("Checking for updated records")

        cursor = self.conn.cursor()
        updatedrecs = cursor.execute(self.deltasql)
        docs = []
        recs = []
        count = 0
        self.totalcount = 0

        for row in updatedrecs.fetchall():
            if count >= chunksize:
                self._updatechunk(cursor, recs, docs)
                self.totalcount += count
                count = 0
                docs = []
                recs = []
            count += 1
            recguid = row[0]
            rechash = row[1] # Note: This is the new hash from tmp table.
            recjson = row[2]
            docid = row[3]
            docrev = row[4]
            doc = simplejson.loads(recjson)
            doc['_id'] = docid
            doc['_rev'] = docrev
            docs.append(doc)
            recs.append((rechash, recjson, docid))
        
        if count > 0:
            self.totalcount += count
            self._updatechunk(cursor, recs, docs)
        
        self.conn.commit()

        logging.info('UPDATE: %s records updated' % self.totalcount)

class DeletedRecords(object):
    def __init__(self, conn, couch):
        self.conn = conn
        self.couch = couch
        self.deletesql = 'delete from %s where recguid=?' % CACHE_TABLE
        self.deltasql = 'SELECT * FROM %s LEFT OUTER JOIN %s USING (recguid) WHERE %s.recguid is null' \
            % (CACHE_TABLE, TMP_TABLE, TMP_TABLE)

    def _deletechunk(self, cursor, recs, docs):
        logging.info('%s deleted' % self.totalcount)
        cursor.executemany(self.deletesql, recs)
        self.conn.commit()
        for doc in docs:
            self.couch.delete(doc)
        
    def execute(self, chunksize):
        logging.info('Checking for deleted records')
        
        cursor = self.conn.cursor()
        deletes = cursor.execute(self.deltasql)
        count = 0
        self.totalcount = 0
        docs = []
        recs = []

        for row in deletes.fetchall():            
            if count >= chunksize:
                self._deletechunk(cursor, recs, docs)
                self.totalcount += count
                count = 0
                docs = []
                recs = []
            count += 1
            recguid = row[0]
            recs.append((recguid,))
            docid = row[3]
            docrev = row[4]
            doc = {'_id': docid, '_rev': docrev}
            docs.append(doc)
        
        if count > 0:
            self.totalcount += count
            self._deletechunk(cursor, recs, docs)

        self.conn.commit()

        logging.info('DELETE: %s records deleted' % self.totalcount)
        
def execute(options):
    conn = setupdb()
    chunksize = int(options.chunksize)
    couch = couchdb.Server(options.couchurl)['vertnet']

    # Loads CSV rows into tmp table:
    TmpTable(conn).insert(options.csvfile, chunksize)

    # Handles new records:
    NewRecords(conn, couch).execute(chunksize)

    # Handles updated records:
    UpdatedRecords(conn, couch).execute(chunksize)

    # Handles deleted records:
    DeletedRecords(conn, couch).execute(chunksize)

    conn.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)    
    
    # Parses command line parameters:
    parser = OptionParser()
    parser.add_option("-f", "--csvfile", dest="csvfile",
                      help="The CSV file",
                      default=None)
    parser.add_option("-u", "--url", dest="couchurl",
                      help="The CouchDB URL",
                      default=None)
    parser.add_option("-c", "--chunk-size", dest="chunksize",
                      help="The chunk size",
                      default=None)

    (options, args) = parser.parse_args()
    
    execute(options)

    #load(options)
