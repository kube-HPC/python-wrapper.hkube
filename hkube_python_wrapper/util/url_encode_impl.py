import sys
if (sys.version_info > (3, 0)):
    # Python 3 code in this block
    from urllib.parse import quote_plus  # pylint: disable=import-error
else:
    # Python 2 code in this block
    from urllib import quote_plus  # pylint: disable=import-error

def url_encode(url):
    return quote_plus(url)
