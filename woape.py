import httplib, urllib
import base64
import json

headers = {"Authorization":"Bearer AAAAAAAAAAAAAAAAAAAAAEBgVAAAAAAAxZcUIQhxg"+
"3gWnlJBMHUJ%2FSYIwbc%3DuIu1L6qBROC2wnONa37BVjw0z35FbjJSL2XuXa8fuCUc8wAWJW"}


params = urllib.urlencode({'q': '@lonlylocly', 'count': '100' })
c = httplib.HTTPSConnection('api.twitter.com')
c.set_debuglevel(0)
#c.request('GET', '/1.1/search/tweets.json?%s'%params, '', headers)

#c.request('GET', '/1.1/statuses/show.json?id=%s'%tweet_id, '', headers)

username = 'lonlylocly'
c.request('GET', '/1.1/statuses/user_timeline.json?screen_name=%s&count=100'%username, '', headers)

resp = c.getresponse()

s = resp.read()
#print s 
s1 = json.loads(s)

for t in s1:
    if t["in_reply_to_status_id"] is not None:
        print t["text"]
#print json.dumps(s1, indent=4)