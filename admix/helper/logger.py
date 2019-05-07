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

        log_format =f'admix_v{admix_version} ' + '%(asctime)s [%(levelname)s] | %(message)s'

        self._loghandler = logging.getLogger('loghandler')
        formatter = logging.Formatter(log_format)
        self._loghandler.setLevel(self.loglevel)

        handler = logging.handlers.RotatingFileHandler(self.logpath, maxBytes=200, backupCount=5)

        fh = logging.FileHandler(filename=self.logpath)
        #fh.setLevel(self.loglevel)
        fh.setFormatter(formatter)
        self._loghandler.addHandler(fh)

        #sh = logging.StreamHandler(sys.stdout)
        #sh.setLevel(self.loglevel)
        #sh.setFormatter(formatter)
        #self._loghandler.addHandler(sh)

        #self._loghandler.info("aDMIX - advanced Data Management in XENON")

    def Info(self, info):
        self._loghandler.info(info)
    def Warning(self, warning):
        self._loghandler.warning(warning)
    def Debug(self, debug):
        self._loghandler.debug(debug)
    def Error(self, error):
        self._loghandler.error(error)
