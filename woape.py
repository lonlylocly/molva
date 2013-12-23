import httplib, urllib
import base64
import json

headers = {"Authorization":"Bearer AAAAAAAAAAAAAAAAAAAAAEBgVAAAAAAAxZcUIQhxg"+
"3gWnlJBMHUJ%2FSYIwbc%3DuIu1L6qBROC2wnONa37BVjw0z35FbjJSL2XuXa8fuCUc8wAWJW"}

#c.request('GET', '/1.1/search/tweets.json?%s'%params, '', headers)
#c.request('GET', '/1.1/statuses/show.json?id=%s'%tweet_id, '', headers)

def get_path(path):
    c = httplib.HTTPSConnection('api.twitter.com')
    c.set_debuglevel(0)
    print "Open path: %s" % path
    resp = c.request('GET', path, '', headers)
    return c.getresponse()

def get_tweet_text(tweet_id) :
    path = '/1.1/statuses/show.json?id=%s'%tweet_id
    resp = get_path(path)
    if resp is not None:
        s = json.loads(resp.read())["text"]
        return s
    else:
        return None

users = ['lonlylocly']
users_seen = ['lonlylocly']
replys = []
f = open('replys.txt', 'w')

while len(replys) < 100 and len(users) > 0:
    print "Start loop iteration"
    print users
    username = users[0]
    users = users[1:]
    
    resp = get_path('/1.1/statuses/user_timeline.json?screen_name=%s&count=30'%username)

    new_users = []
    for t in json.loads(resp.read()):
        if t["in_reply_to_status_id"] is not None:
            print "Got reply-tweet"
            talk = {}
            talk["reply"] = t["text"]
            talk["post"] = get_tweet_text(t["in_reply_to_status_id"])
            print json.dumps(talk, indent=4)
            replys.append(talk)	
            f.write(json.dumps(talk, indent=4) + "\r\n")
            fellow = t["in_reply_to_screen_name"]
            if fellow not in users_seen:
               new_users.append(fellow)
               users_seen.append(fellow)
    users = users + new_users
    

f.close()
#print json.dumps(s1, indent=4)