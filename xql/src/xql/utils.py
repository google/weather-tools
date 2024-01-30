from functools import wraps
from time import gmtime, strftime, time

def timing(f):
    """Measure a time for any function execution."""
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print(f"Query took: { strftime('%H:%M:%S', gmtime(te - ts)) }")
        return result
    return wrap
