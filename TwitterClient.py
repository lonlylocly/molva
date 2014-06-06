import httplib, urllib
import logging
import time

from Exceptions import WoapeException


class TwitterClient:

    def __init__(self, headers):
        self.headers = headers 

    def get_path(self, path):
        c = httplib.HTTPSConnection('api.twitter.com')
        c.set_debuglevel(0)
        logging.info("Open path: %s" % (path))
        c.request('GET', path, '', self.headers)
        resp = c.getresponse()

        return resp
     
    def post_path(self, path, params):
        c = httplib.HTTPSConnection('api.twitter.com')
        c.set_debuglevel(0)
        params_encoded = urllib.urlencode(params)
        headers_post = copy.deepcopy(self.headers)
        headers_post["Content-type"] = "application/x-www-form-urlencoded"
        headers_post["Accept"] = "text/plain"

        logging.info("Open path: %s, body: %s" % (path, params_encoded))
        resp = c.request("POST", path, params_encoded, headers_post)

        return c.getresponse()


    def get_more(self, cur, username, max_id=None, since_id=None):
        resp = None
        max_id_str = "" if max_id is None else "&max_id=" + str(max_id)
        since_id_str = "" if since_id is None  or since_id == 0 else "&since_id=" + str(since_id)

        resp = self.get_path('/1.1/statuses/user_timeline.json?screen_name=%s&count=200%s%s' % (username,max_id_str, since_id_str))

        return resp

