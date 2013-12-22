import httplib, urllib
import base64


k = 'VZgCMryRDlSHTfpbaRzgw'
s = 'HwJAAiHedpajNYt1Vzb3vXe3gMoFAfQ4ELwS81bRaK0'
ks = base64.b64encode(k + ':' + s)

headers = {"Authorization": "Basic %s"%ks,
"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}

#params = urllib.urlencode({'q': '@lonlylocly' })
c = httplib.HTTPSConnection('api.twitter.com')
c.request('POST', '/oauth2/token', 'grant_type=client_credentials', headers)
resp = c.getresponse()
print resp.status, resp.reason
print resp.read()