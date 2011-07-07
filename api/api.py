from google.appengine.ext import deferred
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api import users
from google.appengine.api import taskqueue
from google.appengine.ext.webapp.util import login_required

from ndb import model
from ndb import query

import csv
import logging
import os
import simplejson



# ------------------------------------------------------------------------------
# Models

def urlname(name):
    return '-'.join(''.join(ch for ch in word if ch.isalnum()) \
                        for word in name.split())

class Publisher(model.Model): # key_name=urlname
    """Model for a VertNet data Publisher."""
    name = model.StringProperty('n', required=True)
    urlname = model.ComputedProperty(lambda self: urlname(self.name))
    owner = model.UserProperty('o', required=True)
    admins = model.UserProperty('a', repeated=True)
    created = model.DateTimeProperty('c', auto_now_add=True)
    updated = model.DateTimeProperty('u', auto_now=True)

    @classmethod
    def create(cls, name):
        return Publisher(
            id=urlname(name),
            name=name,
            owner=users.get_current_user())            

class Collection(model.Model): # key_name=urlname, parent=Publisher
    """Model for a collection of records."""
    name = model.StringProperty('n', required=True)
    urlname = model.ComputedProperty(lambda self: urlname(self.name))
    owner = model.UserProperty('o', required=True)
    admins = model.UserProperty('a', repeated=True)
    created = model.DateTimeProperty('c', auto_now_add=True)
    updated = model.DateTimeProperty('u', auto_now=True)

    @classmethod
    def create(cls, name, publisher_key):
        return Collection(
            parent=publisher_key,
            id=urlname(name),
            name=name,
            owner=users.get_current_user())            
    
class Record(model.Model): # key_name=record.occurrenceid, parent=Collection
    """Model for a record."""
    owner = model.UserProperty('u', required=True)
    record = model.TextProperty('r', required=True) # darwin core json representation
    created = model.DateTimeProperty('c', auto_now_add=True)
    updated = model.DateTimeProperty('u', auto_now=True)

    @classmethod
    def create(cls, rec, collection_key):
        return Record(            
            parent=collection_key,
            id=rec['occurrenceid'],
            owner=users.get_current_user(),
            record=simplejson.dumps(rec))
    
class RecordIndex(model.Expando): # parent=Record
    """Index relation for Record."""

    corpus = model.StringProperty('c', repeated=True) # full text

    @classmethod
    def create(cls, rec, collection_key):
        key = model.Key(
            'RecordIndex', 
            rec['occurrenceid'], 
            parent=model.Key('Record', rec['occurrenceid'], parent=collection_key))
        index = RecordIndex(key=key, corpus=cls.getcorpus(rec))
        for concept,value in rec.iteritems():
            index.__setattr__(concept, value.lower())
        return index

    @classmethod
    def search(cls, args, keywords=[]):
        gql = 'SELECT * FROM RecordIndex WHERE'
        for k,v in args.iteritems():
            gql = "%s %s='%s' AND " % (gql, k, v)
        gql = gql[:-5] # Removes trailing AND
        qry = query.parse_gql(gql)[0]
        for keyword in keywords:
            qry = qry.filter(RecordIndex.corpus == keyword)        
        return model.get_multi([x.parent() for x in qry.fetch(keys_only=True)])

    @classmethod
    def getcorpus(cls, rec):
        corpus = set([x.strip().lower() for x in rec.values()]) # verbatim values lower case
        corpus.update(
            reduce(lambda x,y: x+y, 
                   map(lambda x: [s.strip().lower() for s in x.split() if s], rec.values()))) # adds tokenized values
        return list(corpus)
    
# ------------------------------------------------------------------------------
# Map Reduce

def delete_Record(entity):
    entity.key().delete()

def delete_RecordIndex(entity):
    entity.key().delete()

def delete_RecordFullTextIndex(entity):
    entity.key().delete()

# ------------------------------------------------------------------------------
# Handlers

class BaseHandler(webapp.RequestHandler):
    """Base handler for handling common stuff like template rendering."""
    def render_template(self, file, template_args):
        path = os.path.join(os.path.dirname(__file__), "../../templates", file)
        self.response.out.write(template.render(path, template_args))
    def push_html(self, file):
        path = os.path.join(os.path.dirname(__file__), "../../html", file)
        self.response.out.write(open(path, 'r').read())

class ApiHandler(BaseHandler):
    def get(self):
        q = self.request.get('q', None)
        if q:
            keywords = [x.lower() for x in q.split(',')]
            results = RecordFullTextIndex.search(keywords)
        else:
            args = dict((name, self.request.get(name).lower().strip()) for name in self.request.arguments())
            results = RecordIndex.search(args)
        self.response.headers["Content-Type"] = "application/json"
        self.response.out.write(simplejson.dumps([simplejson.loads(x.record) for x in results]))        

class LoadTestData(BaseHandler):
    def post(self):
        self.get()
        
    @login_required
    def get(self):
        pkey = Publisher.create('Museum of Vertebrate Zoology').put()
        ckey = Collection.create('Birds', pkey).put()

        start = int(self.request.get('start'))
        size = int(self.request.get('size'))
        logging.info('start=%s, size=%s' % (start, size))
        count = -1
        created = 0
        path = os.path.join(os.path.dirname(__file__), 'data.csv')
        reader = csv.DictReader(open(path, 'r'), skipinitialspace=True)
        dc = []
        dci = []

        while start >= 0:
            reader.next()
            start -= 1

        for rec in reader:
            if created == size:
                model.put_multi(dc)
                model.put_multi(dci)
            rec = dict((k.lower(), v) for k,v in rec.iteritems()) # lowercase all keys
            dc.append(Record.create(rec, ckey))
            dci.append(RecordIndex.create(rec, ckey))
            created += 1
        self.response.out.write('Done. Created %s records' % created)

application = webapp.WSGIApplication(
         [('/load', LoadTestData),
          ('/api/search', ApiHandler),
          ],
         debug=True)
         
def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
