import sys
if (sys.version_info > (3, 0)):
    # Python 3 code in this block
    from queue import Queue, Empty  # pylint: disable=import-error
else:
    # Python 2 code in this block
    from Queue import Queue, Empty  # pylint: disable=import-error
__all__ = ['Queue', 'Empty']
