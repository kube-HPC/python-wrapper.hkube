from six.moves import urllib
def url_encode(url):
    return urllib.parse.quote_plus(url)
