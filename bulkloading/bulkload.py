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
from couchdb import Server

DB_FILE = 'bulk.sqlite3.db'
CACHE_TABLE = 'cache'
TMP_TABLE = 'tmp'

def setupdb(conn):
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

def load(csvfile, couchdb_url):
    conn = sqlite3.connect(DB_FILE)
    setupdb(conn)
    c = conn.cursor()
    server = Server(couchdb_url)
    vertnetdb = server['vertnet']
    dr = csv.DictReader(open(csvfile, 'r'), skipinitialspace=True)

    # Inserts csv data into tmp db table:
    for row in dr:
        recguid = row['SpecimenGUID']
        
        # Creates record hash:
        cols = row.keys()
        cols.sort()
        fields = [row[x].strip() for x in cols]
        line = reduce(lambda x,y: '%s%s' % (x, y), fields)
        rechash = hashlib.sha224(line).hexdigest()

        recjson = simplejson.dumps(row)
        sql = "insert into %s values ('%s', '%s', '%s')"
        c.execute(sql % (TMP_TABLE, recguid, rechash, recjson))
    conn.commit()

    # Handles new records:
    sql = "SELECT * FROM %s LEFT OUTER JOIN %s USING (recguid) WHERE %s.recguid is null"
    new = c.execute(sql % (TMP_TABLE, CACHE_TABLE, CACHE_TABLE))
    records = []
    for row in new.fetchall():
        recguid = row[0]
        rechash = row[1]
        docid = uuid4().hex
        recjson = row[2]
        doc = simplejson.loads(recjson)
        doc['_id'] = docid
        records.append(doc)
        logging.info('NEW record docid %s' % docid)
        sql = "insert into %s values ('%s', '%s', '%s', '%s', '%s')"             
        c.execute(sql % (CACHE_TABLE, recguid, rechash, recjson, docid, '')) 
    conn.commit()
    if len(records) > 0:
        # Bulkloads docs to CouchDB and sets first cache.docrev:
        for doc in vertnetdb.update(records):   
            sql = 'update %s set docrev="%s" where docid="%s"'
            c.execute(sql % (CACHE_TABLE, doc[2], doc[1]))
    conn.commit()

    # Handles updated records:
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

        sql = "update %s set rechash='%s', recjson='%s' where docid='%s'"
        c.execute(sql % (CACHE_TABLE, rechash, recjson, docid))        
    conn.commit()
    
    if len(records) > 0:
        # Bulkloads updates to CouchDB and sets new cache.docrev:
        for doc in vertnetdb.update(records):   
            sql = 'update %s set docrev="%s" where docid="%s"'
            c.execute(sql % (CACHE_TABLE, doc[2], doc[1]))
    conn.commit()

    # Handles deleted records:
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
        sql = 'delete from %s where recguid="%s"' % (CACHE_TABLE, recguid)
        c.execute(sql)
    conn.commit()

    conn.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)    
    
    # Parses command line parameters:
    parser = OptionParser()
    parser.add_option("-f", "--csvfile", dest="csvfile",
                      help="The CSV file",
                      default=None)
    parser.add_option("-u", "--url", dest="url",
                      help="The CouchDB URL",
                      default=None)

    (options, args) = parser.parse_args()
    csvfile = options.csvfile
    url = options.url
    
    # Writes the CSV file:
    load(csvfile, url)
