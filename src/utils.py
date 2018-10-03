"""utils.py
support function
"""
from datetime import datetime
from functools import wraps

import logme

@logme.log
def profile(_callable, logger=None):
    """function profiling the cost time"""
    @wraps(_callable)
    def wrapped(*args, **kwargs):
        start_time = datetime.now()
        result = _callable(*args, **kwargs)
        cost_time = datetime.now() - start_time
        logger.debug('%s.%s completed in %s', \
            _callable.__module__, _callable.__name__, str(cost_time))
        return result
    return wrapped
