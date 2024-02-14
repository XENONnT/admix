# -*- coding: utf-8 -*-

"""Top-level package for aDMIX."""
__version__ = '1.0.14'

import os
import logging
from utilix import uconfig

def get_logger():
    logger = logging.getLogger("admix")
    ch = logging.StreamHandler()
    ch.setLevel(uconfig.logging_level)
    logger.setLevel(uconfig.logging_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

logger = get_logger()

PKGDIR = os.path.dirname(__file__)

DEFAULT_CONFIG = os.path.join(PKGDIR, 'config', 'default.config')

from . import utils
from .downloader import download
from .uploader import upload
from .uploader import preupload
from . import manager
from . import monitor
from . import validator
