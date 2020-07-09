import logging
import logging.handlers
import sys
from admix.helper.helper import global_dictionary, get_hostconfig
from admix import __version__ as admix_version

class Logger():

    def __init__(self,
                 logpath=None,
                 loglevel=logging.INFO,
                 ):

        if logpath != None:
            self.logpath = logpath
        else:
            self.logpath = "./"
        self.loglevel = loglevel
        self._loghandler = None
        self.setup()

    def setup(self):
        """Setup your logging handler is crucial

        """
        log_format = f'admix_v{admix_version} ' + '%(asctime)s [%(levelname)s]\t| %(message)s'

        self._loghandler = logging.getLogger('admix_logger')
        self._loghandler.setLevel(logging.DEBUG)
        # create file handler which logs even debug messages
        fh = logging.FileHandler(self.logpath)
        fh.setLevel(logging.DEBUG)
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # create formatter and add it to the handlers
        formatter = logging.Formatter(log_format)
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # add the handlers to the logger
        self._loghandler.addHandler(fh)
        self._loghandler.addHandler(ch)

    def Info(self, info):
        self._loghandler.info(info)
    def Warning(self, warning):
        self._loghandler.warning(warning)
    def Debug(self, debug):
        self._loghandler.debug(debug)
    def Error(self, error):
        self._loghandler.error(error)
