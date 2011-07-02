from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api import users

from ndb import model
from ndb import query

import csv
import logging
import os
import simplejson



# ------------------------------------------------------------------------------
# Models

class DarwinCore(model.Model):
    owner = model.UserProperty('u', required=True)
    record = model.TextProperty('r', required=True) # json
    created = model.DateTimeProperty('c', auto_now_add=True)
    updated = model.DateTimeProperty('u', auto_now=True)
    @classmethod
    def create(cls, rec):
        return DarwinCore(
            id=rec['occurrenceid'],
            owner=users.get_current_user(),
            record=simplejson.dumps(rec))
    
class DarwinCoreIndex(model.Expando): # parent=DarwinCore
    """Contains indexed properties for all concepts in the parent DarwinCore.record."""
    @classmethod
    def create(cls, rec):
        parent = model.Key(DarwinCore, rec['occurrenceid'])
        dci = DarwinCoreIndex(parent=parent)        
        for key in rec.keys():
            dci.__setattr__(key, rec.get(key))
        return dci

    @classmethod
    def search(cls, args):
        gql = 'SELECT * FROM DarwinCoreIndex WHERE'
        for k,v in args.iteritems():
            gql = "%s %s='%s' AND " % (gql, k, v)
        gql = gql[:-5] # Removes trailing AND
        qry = query.parse_gql(gql)[0]
        return model.get_multi([x.parent() for x in qry.fetch(keys_only=True)])

class DarwinCoreFullTextIndex(model.Model): # parent=DarwinCore
    """Contains the full text of a Darwin Core record."""
    corpus = model.StringProperty('c', repeated=True)
    @classmethod
    def create(cls, rec):
        parent = model.Key(DarwinCore, rec['occurrenceid'])
        logging.info(str(rec))
        corpus = set([x.strip().lower() for x in rec.values()]) # verbatim values lower case
        corpus.update(
            reduce(lambda x,y: x+y, 
                   map(lambda x: [s.strip().lower() for s in x.split() if s], rec.values()))) # adds tokenized values
        logging.info(str(corpus))
        return DarwinCoreFullTextIndex(parent=parent, corpus=list(corpus))
    
    @classmethod
    def search(cls, keywords):
        qry = DarwinCoreFullTextIndex.query()
        for keyword in keywords:
            qry = qry.filter(DarwinCoreFullTextIndex.corpus == keyword)
        logging.info(str(qry))
        return model.get_multi([x.parent() for x in qry.fetch(keys_only=True)])
    

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
            results = DarwinCoreFullTextIndex.search(keywords)
        else:
            args = dict((name, self.request.get(name)) for name in self.request.arguments())
            logging.info(args)
            results = DarwinCoreIndex.search(args)
        self.response.headers["Content-Type"] = "application/json"
        self.response.out.write(simplejson.dumps([simplejson.loads(x.record) for x in results]))        

class LoadTestData(BaseHandler):
    def get(self):
        path = os.path.join(os.path.dirname(__file__), 'data.csv')
        reader = csv.DictReader(open(path, 'r'), skipinitialspace=True)
        for rec in reader:
            rec = dict((k.lower(), v) for k,v in rec.iteritems()) # lowercase all keys
            model.put_multi([
                    DarwinCore.create(rec),
                    DarwinCoreIndex.create(rec),
                    DarwinCoreFullTextIndex.create(rec),
                    DarwinCoreSimpleIndex.create(rec)])
            
application = webapp.WSGIApplication(
         [('/load', LoadTestData),
          ('/api/search', ApiHandler),
          ],
         debug=True)
         
def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
