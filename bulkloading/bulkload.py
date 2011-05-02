#!/usr/bin/env python

import csv
import hashlib
import logging
from optparse import OptionParser
import re
import sqlite3

DB_FILE = 'bulk.sqlite3.db'
DB_CACHE_TABLE = 'cache'
DB_TMP_TABLE = 'tmp'

def setupdb(conn):
    c = conn.cursor()
    c.execute('create table if not exists %s (guid text, hash text, id text)' % DB_CACHE_TABLE)
    c.execute('create table if not exists %s (guid text, hash text, id text)' % DB_TMP_TABLE)
    c.execute('delete from %s' % DB_TMP_TABLE)
    c.close()

def load(csvfile):
    conn = sqlite3.connect(DB_FILE)
    setupdb(conn)
    c = conn.cursor()
    dr = csv.DictReader(open(csvfile, 'r'), skipinitialspace=True)

    # Inserts csv data into tmp db table:
    for row in dr:
        guid = row['SpecimenGUID']
        cols = row.keys()
        cols.sort()
        fields = [row[x].strip() for x in cols]
        # Munges all fields into single line:
        line = reduce(lambda x,y: '%s%s' % (x, y), fields)
        hashdigest = hashlib.sha224(line).hexdigest()
        c.execute("insert into %s values ('%s', '%s', '')" % (DB_TMP_TABLE, guid, hashdigest))
    conn.commit()

    # Handles new records:
    sql = "SELECT * FROM %s LEFT OUTER JOIN %s USING (guid) WHERE %s.guid is null"
    new = c.execute(sql % (DB_TMP_TABLE, DB_CACHE_TABLE, DB_CACHE_TABLE))
    logging.info('NEW')
    for row in new:
        logging.info(row)
        # TODO: Insert to CouchDb and cache table

    # Handles updated records:
    sql = "SELECT * FROM %s, %s WHERE %s.guid = %s.guid AND %s.hash <> %s.hash"
    updates = c.execute(sql % (DB_TMP_TABLE, DB_CACHE_TABLE, DB_TMP_TABLE, 
                               DB_CACHE_TABLE, DB_TMP_TABLE, DB_CACHE_TABLE))
    logging.info('UPDATED')
    for row in updates:
        logging.info(row)
        # TODO: Update CouchDb and cache table

    # Handles deleted records:
    sql = "SELECT * FROM %s LEFT OUTER JOIN %s USING (guid) WHERE %s.guid is null"
    deletes = c.execute(sql % (DB_CACHE_TABLE, DB_TMP_TABLE, DB_TMP_TABLE))
    logging.info('DELETED')
    for row in deletes:
        logging.info(row)
        # TODO: Delete from CouchDb and cache table

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)    
    
    # Parses command line parameters:
    parser = OptionParser()
    parser.add_option("-f", "--csvfile", dest="csvfile",
                      help="The CSV file",
                      default=None)
    (options, args) = parser.parse_args()
    csvfile = options.csvfile
    
    # Writes the CSV file:
    load(csvfile)

