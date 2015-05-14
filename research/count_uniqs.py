#!/usr/bin/python

f = open('/tmp/users.txt','r')

users = {}

while True:
    l = f.readline()
    if l is None:
        break
    if l not in users:
        users[l] = 0
    users[l] += 1

print "users cnt: %s" % len(users)

for u in sorted(users.keys(), key=lambda x: users[x], reverse=True)[:10]:
    print "%s: %s" % (u, users[u])
