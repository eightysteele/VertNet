#!/usr/bin/env python

import copy
import csv
import hashlib
import logging
from optparse import OptionParser
import re
import simplejson
import sqlite3
from uuid import uuid4

# CouchDB imports
import couchdb

DB_FILE = 'bulk.sqlite3.db'
CACHE_TABLE = 'cache'
TMP_TABLE = 'tmp'

def setupdb():
    conn = sqlite3.connect(DB_FILE)
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
        self.table = 'tmp'
        self.conn = conn

    def _insertchunk(self, rows, cursor):
        logging.info('Inserting %s records to tmp table.' % len(rows))

        sql = 'insert into %s values (?, ?, ?)' % self.table

        for row in rows:
            recguid = row['occurrenceID']
            cols = row.keys()
            cols.sort()
            fields = [row[x].strip() for x in cols]
            line = reduce(lambda x,y: '%s%s' % (x, y), fields)
            rechash = hashlib.sha224(line).hexdigest()
            recjson = simplejson.dumps(row)
            cursor.execute(sql, (recguid, rechash, recjson))

        self.conn.commit()
        
    def insert(self, csvfile, chunksize):
        logging.info('Inserting %s to tmp table.' % csvfile)

        rows = []
        count = 0
        totalcount = 0
        cursor = self.conn.cursor()
        reader = csv.DictReader(open(csvfile, 'r'), skipinitialspace=True)

        for row in reader:
            if count >= chunksize:
                self._insertchunk(rows, cursor)
                totalcount += count
                count = 0
                rows = []        
            rows.append(row)
            count += 1

        if count > 0:
            totalcount += count
            self._insertchunk(rows, cursor)

        logging.info('%s rows inserted to tmp table' % totalcount)
 
    
class NewRecords(object):

    def __init__(self, conn):
        self.conn = conn
    
    def _insertcouch(self, docs, couch, cursor):
        """Bulk inserts docs to couchdb and updates cache table doc revision."""
        logging.info('Inserting %s docs to CouchDB' % len(docs))
        sql = 'update %s set docrev=? where docid=?' % CACHE_TABLE
        for doc in couch.update(docs):   
            cursor.execute(sql, (doc[2], doc[1]))
        self.conn.commit()

    def execute(self, couch, chunksize):
        logging.info("Checking for new records")

        insertsql = 'insert into %s values (?, ?, ?, ?, ?)' % CACHE_TABLE            
        cursor = self.conn.cursor()
        deltasql = "SELECT * FROM %s LEFT OUTER JOIN %s USING (recguid) WHERE %s.recguid is null"
        newrecs = cursor.execute(deltasql % (TMP_TABLE, CACHE_TABLE, CACHE_TABLE))
        docs = []
        count = 0
        totalcount = 0

        for row in newrecs.fetchall():
            if count >= chunksize:
                self._insertcouch(docs, couch, cursor)
                totalcount += count
                count = 0
                docs = []
            count += 1
            recguid = row[0]
            rechash = row[1]
            docid = uuid4().hex
            recjson = row[2]
            doc = simplejson.loads(recjson)
            doc['_id'] = docid
            docs.append(doc)
            cursor.execute(insertsql, (recguid, rechash, recjson, docid, ''))

        if count > 0:
            totalcount += count
            self._insertcouch(docs, couch, cursor)
            
        self.conn.commit()
        logging.info('INSERT: %s records inserted' % totalcount)
    

class UpdatedRecords(object):

    def __init__(self, conn):
        self.conn = conn

    def _insertcouch(self, docs, couch, cursor):
        """Bulk inserts docs to couchdb and updates cache table doc revision."""
        logging.info('Updating %s docs to CouchDB' % len(docs))
        sql = 'update %s set docrev=? where docid=?' % CACHE_TABLE
        for doc in couch.update(docs):   
            cursor.execute(sql, (doc[2], doc[1]))
        self.conn.commit()

    def execute(self, couch, chunksize):
        logging.info("Checking for updated records")

        udpatesql = 'update %s set rechash=?, recjson=? where docid=?' % CACHE_TABLE
        deltasql = 'SELECT c.recguid, t.rechash, c.recjson, c.docid, c.docrev FROM %s as t, %s as c WHERE t.recguid = c.recguid AND t.rechash <> c.rechash' % (TMP_TABLE, CACHE_TABLE)
        cursor = self.conn.cursor()
        updatedrecs = cursor.execute(deltasql)
        docs = []
        count = 0
        totalcount = 0

        for row in updatedrecs.fetchall():
            if count >= chunksize:
                self._insertcouch(docs, couch, cursor)
                totalcount += count
                count = 0
                docs = []
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
            cursor.execute(udpatesql, (rechash, recjson, docid))        
        
        if count > 0:
            totalcount += count
            self._insertcouch(docs, couch, cursor)
        
        self.conn.commit()

        logging.info('UPDATE: %s records updated' % totalcount)

class DeletedRecords(object):
    def __init__(self, conn):
        self.conn = conn

    def execute(self, couch):
        logging.info('Checking for deleted records')
        
        deletesql = 'delete from %s where recguid=?' % CACHE_TABLE
        deltasql = 'SELECT * FROM %s LEFT OUTER JOIN %s USING (recguid) WHERE %s.recguid is null'
        cursor = self.conn.cursor()
        deletes = cursor.execute(deltasql % (CACHE_TABLE, TMP_TABLE, TMP_TABLE))
        count = 0

        for row in deletes.fetchall():            
            count += 1
            recguid = row[0]
            docid = row[3]
            docrev = row[4]
            couch.delete({'_id': docid, '_rev': docrev})
            logging.info('%s, %s' % (deletesql, recguid))
            cursor.execute(deletesql, (recguid,))

        self.conn.commit()

        logging.info('DELETE: %s records deleted' % count)
        
def execute(options):
    conn = setupdb()
    chunksize = int(options.chunksize)
    couch = couchdb.Server(options.couchurl)['vertnet']

    # Loads CSV rows into tmp table:
    TmpTable(conn).insert(options.csvfile, chunksize)

    # Handles new records:
    NewRecords(conn).execute(couch, chunksize)

    # Handles updated records:
    UpdatedRecords(conn).execute(couch, chunksize)

    # Handles deleted records:
    DeletedRecords(conn).execute(couch)

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
