from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app
import logging
import os
import simplejson
import urllib
import urllib2

class BaseHandler(webapp.RequestHandler):
    def push_html(self, file):
        path = os.path.join(os.path.dirname(__file__), "content", file)
        self.response.out.write(open(path, 'r').read())

class ContentHandler(BaseHandler):
    def get(self):
        self.push_html('index.html')

application = webapp.WSGIApplication(
         [('/', ContentHandler),], debug=True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
