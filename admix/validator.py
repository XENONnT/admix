"""
Validate that the data can be loaded successfully and move the data to the production folder.

Copied shamelessly from reprox: https://github.com/XENONnT/reprox/blob/master/reprox/validate_run.py

But removed strax + reprox dependency and made a function instead of a class
"""

import typing as ty
import json
from warnings import warn
import os
import shutil
import glob
from collections import defaultdict


class StraxDataValidation:
    """
    Check that a directory (corresponding to a single datatype is
    """

    def __init__(self, path: str):
        self.path = path

    def find_error(self) -> str:
        """Run several checks on a path to see if the processing was done correctly"""
        if self._is_temp():
            return 'is_temp_folder'

        md = self._open_metadata()
        if not md:
            return 'has_no_metadata'

        if self._did_fail(md):
            return 'has_exception'

        if self._misses_chunks(md):
            return 'misses_chunks'

        key = os.path.split(self.path)[-1]
        split_key = key.split('-')

        if self._wrong_format(split_key):
            return 'is_wrong_format'

        return False

    def _is_temp(self):
        if '_temp' in self.path:
            warn(f'{self.path} is not finished', UserWarning)
            return True

    def _did_fail(self, md):
        if 'exception' in md:
            warn(f'{self.path} has an exception', UserWarning)
            return True

    def _misses_chunks(self, md):
        n_files = os.listdir(self.path)
        n_chunks = len(n_files) - 1  # metadata
        chunks = [c.get('n') > 0 for c in md['chunks']]
        if n_chunks != sum(chunks):
            warn(f'{self.path} misses chunks?!', UserWarning)
            return True

    @staticmethod
    def _wrong_format(split_key):
        if len(split_key) != 3:
            warn(f'{split_key} is not correct format?!', UserWarning)
            return True
        return False

    def _open_metadata(self) -> dict:
        files = glob.glob(os.path.join(self.path, '*'))
        for f in files:
            if 'metadata' in f:
                md_path = f
                break
        else:
            return {}

        with open(md_path, mode='r') as f:
            return json.loads(f.read())
