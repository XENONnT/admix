"""
Utility functions/objects used by admix generally
"""
from utilix import xent_collection, xe1t_collection, DB

from . import logger

try:
    db = DB()
except:
    logger.warning(f"Initializing utilix DB failed. You cannot do database operations")
    db = None
xent_runs_collection = xent_collection()
xent_context_collection = xent_collection('contexts')
xe1t_runs_collection = xe1t_collection()


def make_did(run_number, dtype, hash):
    ### HUGE WARNING: DO NOT CHANGE THIS FUNCTION!!!!
    ## IT WILL BREAK LITERALLY EVERYTHING
    scope = 'xnt_%06d' % run_number
    dataset = "%s-%s" % (dtype, hash)
    return "%s:%s" % (scope, dataset)
    ### HUGE WARNING: DO NOT CHANGE THIS FUNCTION!!!!


def from_did(did):
    """Takes a did and returns the run number, dtype, hash"""
    scope, name = did.split(':')
    number = int(scope.split('_')[1])
    dtype, h = name.split('-')
    return number, dtype, h
