# -*- coding: utf-8 -*-

"""Top-level package for aDMIX."""
__author__ = """Boris Bauermeister"""
__email__ = 'Boris.Bauermeister@gmail.com'
__version__ = '0.2.0'

#interfaces:
import os
from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.destination import Destination
from admix.interfaces.keyword import Keyword
from admix.interfaces.templater import Templater


#tasks:
PKGDIR = os.path.dirname(__file__)

DEFAULT_CONFIG = os.path.join(PKGDIR, 'config', 'default.config')

from admix.download import download

