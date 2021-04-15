# -*- coding: utf-8 -*-

"""Top-level package for aDMIX."""
__author__ = """Boris Bauermeister"""
__email__ = 'Boris.Bauermeister@gmail.com'
__version__ = '0.3.1'

#interfaces:
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

from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.destination import Destination
from admix.interfaces.keyword import Keyword
from admix.interfaces.templater import Templater






#tasks:
PKGDIR = os.path.dirname(__file__)

DEFAULT_CONFIG = os.path.join(PKGDIR, 'config', 'default.config')

from admix.download import download

