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
    
class NewRecordsHandler(object):

    def __init__(self, conn):
        self.conn = conn
    
    def _insertcouch(self, docs, couch, cursor):
        """Bulk inserts docs to couchdb and updates cache table doc revision."""
        logging.info('Bulk updating %s docs to CouchDB' % len(docs))
        sql = 'update %s set docrev=? where docid=?' % CACHE_TABLE
        for doc in couch.update(docs):   
            cursor.execute(sql, (doc[2], doc[1]))
        self.conn.commit()

    def execute(self, couch, chunksize):
        logging.info("Handling new records in CSV")

        cursor = self.conn.cursor()
        sql = "SELECT * FROM %s LEFT OUTER JOIN %s USING (recguid) WHERE %s.recguid is null"
        newrecs = cursor.execute(sql % (TMP_TABLE, CACHE_TABLE, CACHE_TABLE))
        docs = []
        count = 0
        totalcount = 0

        sql = 'insert into %s values (?, ?, ?, ?, ?)' % CACHE_TABLE            
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
            cursor.execute(sql, (recguid, rechash, recjson, docid, ''))

        if count > 0:
            totalcount += count
            self._insertcouch(docs, couch, cursor)
            
        self.conn.commit()
        logging.info('Handled %s new records in CSV' % totalcount)
    

def execute(options):
    conn = setupdb()
    chunksize = int(options.chunksize)
    couch = couchdb.Server(options.couchurl)['vertnet']

    # Loads CSV rows into tmp table:
    TmpTable(conn).insert(options.csvfile, chunksize)

    # Handles new records:
    NewRecordsHandler(conn).execute(couch, chunksize)

def load(options):
    conn = setupdb(conn)
    tmploader = TmpTableLoader(conn)
    tmploader.insert(options.csvfile, int(options.chunksize))

    c = conn.cursor()
    server = Server(couchdb_url)
    vertnetdb = server['vertnet']
    dr = csv.DictReader(open(csvfile, 'r'), skipinitialspace=True)
    sql = "insert into %s values (?, ?, ?)" % TMP_TABLE 

    chunksize = 10 * 1000
    count = 0
    rows = []

    # Loads CSV rows into sqlite tmp table in chunks:
    for row in dr:
        if count >= chunksize:
            for r in rows:
                recguid = row['occurrenceID']
                cols = row.keys()
                cols.sort()
                fields = [row[x].strip() for x in cols]
                line = reduce(lambda x,y: '%s%s' % (x, y), fields)
                rechash = hashlib.sha224(line).hexdigest()
                recjson = simplejson.dumps(row)
                sql = "insert into %s values (?, ?, ?)" % TMP_TABLE 
                c.execute(sql, (recguid, rechash, recjson))
            conn.commit()
            count = 0
            rows = []        
        else:
            rows.append(row)
            count += 1
            
    

    



    logging.info('Inserting records to SQLite')
    conn = sqlite3.connect(DB_FILE)
    setupdb(conn)
    c = conn.cursor()
    server = Server(couchdb_url)
    vertnetdb = server['vertnet']
    dr = csv.DictReader(open(csvfile, 'r'), skipinitialspace=True)
    sql = "insert into %s values (?, ?, ?)" % TMP_TABLE 
    #c.executemany(sql, tmprecgen(dr))
    # Inserts csv data into tmp db table:
    for row in dr:
        recguid = row['occurrenceID']
        
        # Creates record hash:
        cols = row.keys()
        cols.sort()
        fields = [row[x].strip() for x in cols]
        line = reduce(lambda x,y: '%s%s' % (x, y), fields)
        rechash = hashlib.sha224(line).hexdigest()
        recjson = simplejson.dumps(row)
        sql = "insert into %s values (?, ?, ?)" % TMP_TABLE 
        c.execute(sql, (recguid, rechash, recjson))
    conn.commit()

    # Handles new records:
    
    sql = "SELECT * FROM %s LEFT OUTER JOIN %s USING (recguid) WHERE %s.recguid is null"
    new = c.execute(sql % (TMP_TABLE, CACHE_TABLE, CACHE_TABLE))
    logging.info("Calculating new records")
    records = []
    for row in new.fetchall():
        recguid = row[0]
        rechash = row[1]
        docid = uuid4().hex
        recjson = row[2]
        doc = simplejson.loads(recjson)
        doc['_id'] = docid
        records.append(doc)
        #logging.info('NEW record docid %s' % docid)
        sql = "insert into %s values (?, ?, ?, ?, ?)" % CACHE_TABLE            
        c.execute(sql, (recguid, rechash, recjson, docid, ''))
    conn.commit()

    if len(records) > 0:
        batchsize = 1 * 1000
        batch = []
        count = 0
        for rec in records:
            if count >= batchsize:
                logging.info('Bulk updating to CouchDB...')
                for doc in vertnetdb.update(batch):   
                    sql = 'update %s set docrev=? where docid=?' % CACHE_TABLE
                    c.execute(sql, (doc[2], doc[1]))
                count = 0
                batch = []
            else:
                batch.append(rec)
                count += 1
        if len(batch) > 0:
            logging.info('Bulk updating remainders to CouchDB...')
            for doc in vertnetdb.update(batch):   
                sql = 'update %s set docrev=? where docid=?' % CACHE_TABLE
                c.execute(sql, (doc[2], doc[1]))


#    if len(records) > 0:
        # Bulkloads docs to CouchDB and sets first cache.docrev:
#        for doc in vertnetdb.update(records):   
#            sql = 'update %s set docrev=? where docid=?' % CACHE_TABLE
#            c.execute(sql, (doc[2], doc[1]))
    conn.commit()
    

    # Handles updated records:
    logging.info('Checking for updated records')
    sql = 'SELECT c.recguid, t.rechash, c.recjson, c.docid, c.docrev FROM %s as t, %s as c WHERE t.recguid = c.recguid AND t.rechash <> c.rechash' % (TMP_TABLE, CACHE_TABLE)

    updates = c.execute(sql)
    records = []
    for row in updates.fetchall():
        recguid = row[0]
        rechash = row[1] # Note: This is the new hash from tmp table.
        recjson = row[2]
        docid = row[3]
        docrev = row[4]
            
        logging.info('UPDATE docid %s' % docid)

        doc = simplejson.loads(recjson)
        doc['_id'] = docid
        doc['_rev'] = docrev
        records.append(doc)

        sql = "update %s set rechash=?, recjson=? where docid=?" % CACHE_TABLE
        c.execute(sql, (rechash, recjson, docid))        
    conn.commit()
    
    if len(records) > 0:
        batchsize = 25 * 1000
        batch = []
        count = 0
        for rec in records:
            if count >= batchsize:
                logging.info('Bulk updating to CouchDB...')
                for doc in vertnetdb.update(batch):   
                    sql = 'update %s set docrev=? where docid=?' % CACHE_TABLE
                    c.execute(sql, (doc[2], doc[1]))
                count = 0
                batch = []
                continue                
            batch.append(rec)
            count += 1
        if len(batch) > 0:
            logging.info('Bulk updating to CouchDB...')
            for doc in vertnetdb.update(batch):   
                sql = 'update %s set docrev=? where docid=?' % CACHE_TABLE
                c.execute(sql, (doc[2], doc[1]))

    conn.commit()

    # Handles deleted records:
    logging.info('Checking for deleted records')
    sql = "SELECT * FROM %s LEFT OUTER JOIN %s USING (recguid) WHERE %s.recguid is null"
    deletes = c.execute(sql % (CACHE_TABLE, TMP_TABLE, TMP_TABLE))
    for row in deletes.fetchall():
        recguid = row[0]
        rechash = row[1]
        recjson = row[2]
        docid = row[3]
        docrev = row[4]

        logging.info('DELETE docid %s' % docid)

        vertnetdb.delete({'_id': docid, '_rev': docrev})
        sql = 'delete from %s where recguid=?' % CACHE_TABLE
        c.execute(sql, (recguid))
    conn.commit()

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
