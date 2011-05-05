#!/usr/bin/env python

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
DB_CACHE_TABLE = 'cache'
DB_TMP_TABLE = 'tmp'

def setupdb(conn):
    c = conn.cursor()
    c.execute('create table if not exists %s (guid text, hash text, uuid text, data text)' % DB_CACHE_TABLE)
    c.execute('create table if not exists %s (guid text, hash text, uuid text, data text)' % DB_TMP_TABLE)
    c.execute('delete from %s' % DB_TMP_TABLE)
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
        guid = row['SpecimenGUID']
        uuid = uuid4().hex
        cols = row.keys()
        cols.sort()
        fields = [row[x].strip() for x in cols]
        # Munges all fields into single line:
        line = reduce(lambda x,y: '%s%s' % (x, y), fields)
        hashdigest = hashlib.sha224(line).hexdigest()
        data = simplejson.dumps(row)
        sql = "insert into %s values ('%s', '%s', '%s', '%s')"
        c.execute(sql % (DB_TMP_TABLE, guid, hashdigest, uuid, data))

    # Handles new records:
    sql = "SELECT * FROM %s LEFT OUTER JOIN %s USING (guid) WHERE %s.guid is null"
    new = c.execute(sql % (DB_TMP_TABLE, DB_CACHE_TABLE, DB_CACHE_TABLE))
    for row in new.fetchall():
        guid = row[0]
        hashdigest = row[1]
        uuid = row[2]
        data = simplejson.loads(row[3])
        logging.info('NEW record detected: Saving GUID %s to CouchDB and local cache' % guid)
        vertnetdb.save(data)
        sql = 'insert into %s values ("%s", "%s", "%s", "%s")'
        c.execute(sql % (DB_CACHE_TABLE, guid, hashdigest, uuid, str(data)))

    # Handles updated records:
    sql = "SELECT * FROM %s, %s WHERE %s.guid = %s.guid AND %s.hash <> %s.hash"
    updates = c.execute(sql % (DB_TMP_TABLE, DB_CACHE_TABLE, DB_TMP_TABLE, 
                               DB_CACHE_TABLE, DB_TMP_TABLE, DB_CACHE_TABLE))
    for row in updates.fetchall():
        logging.info(row)
        # TODO: Update CouchDb and cache table

    # Handles deleted records:
    sql = "SELECT * FROM %s LEFT OUTER JOIN %s USING (guid) WHERE %s.guid is null"
    deletes = c.execute(sql % (DB_CACHE_TABLE, DB_TMP_TABLE, DB_TMP_TABLE))
    for row in deletes.fetchall():
        logging.info(row)
        # TODO: Delete from CouchDb and cache table

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
