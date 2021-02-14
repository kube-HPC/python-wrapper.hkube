import math
import functools
from hkube_python_wrapper.util.logger import log


def percentile(N, percent, key=lambda x: x):
    """
    Find the percentile of a list of values.

    @parameter N - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0 to 100
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not N:
        return None
    percent = percent/100.0
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0+d1


# median is 50th percentile.
median = functools.partial(percentile, percent=0.5)


def print_percentiles(values, percentiles=[10, 25, 50, 75, 90, 95], title=None):
    if not values:
        return
    if (title):
        log.debug(title)
        log.debug('-'*len(title))
    for per_name in percentiles:
        log.debug('{per_name}, {per_value}', per_name=per_name, per_value=str(percentile(values, per_name)))
