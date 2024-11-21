"""
Utility functions/objects used by admix generally
"""
from utilix import xent_collection, xe1t_collection, DB

from . import logger

try:
    db = DB()
except:
    print("DB initialization failed")
    logger.warning(f"Initializing utilix DB failed. You cannot do database operations")
    db = None


class LazyInit:

    def __init__(self, function, *args, **kwargs):
        self._instance = None
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def _init_instance(self):
        if self._instance is None:
            self._instance = self.function(*self.args, **self.kwargs)

    def __getattr__(self, name):
        self._init_instance()  # Initialize only when an attribute is accessed
        return getattr(self._instance, name)


xent_runs_collection = LazyInit(xent_collection)
xent_context_collection = LazyInit(xent_collection, 'contexts')
xe1t_runs_collection = LazyInit(xe1t_collection)


RAW_DTYPES = ['raw_records',
              'raw_records_he',
              'raw_records_mv',
              'raw_records_nv',
              'raw_records_aqmon',
              'raw_records_aux_mv',
              'raw_records_aqmon_nv'
              ]

RAWish_DTYPES = RAW_DTYPES + ['records',
                              'records_he',
                              'records_nv',
                              'raw_records_coin_nv',
                              'lone_raw_records_nv',
                              'lone_raw_record_statistics_nv',
                              'records_mv'
                              ]


def make_did(run_number, dtype, hash):
    ### HUGE WARNING: DO NOT CHANGE THIS FUNCTION!!!!
    ## IT WILL BREAK LITERALLY EVERYTHING
    scope = 'xnt_%06d' % run_number
    dataset = "%s-%s" % (dtype, hash)
    return "%s:%s" % (scope, dataset)
    ### HUGE WARNING: DO NOT CHANGE THIS FUNCTION!!!!


def parse_did(did):
    """Takes a did and returns the run number, dtype, hash"""
    scope, name = did.split(':')
    number = int(scope.split('_')[1])
    dtype, h = name.split('-')
    return number, dtype, h


def make_highlevel_container_did(run_number, straxen_version):
    scope = 'xnt_%06d' % run_number
    if not straxen_version.startswith('v'):
        straxen_version = 'v' + straxen_version
    container_name = 'highlevel_' + straxen_version.replace('.', '-')
    return f"{scope}:{container_name}"


def parse_dirname(dirname):
    number, dtype, lineage_hash = dirname.split('-')
    number = int(number)
    return number, dtype, lineage_hash


