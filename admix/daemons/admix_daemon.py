"""
Base class information for different admix daemons
"""
import time
from utilix import xent_collection


class AdmixDaemon:
    query = {}

    def __init__(self, db_query=None):

        if db_query is None:
            # then we loop over all data
            db_query = {}

        self.query = db_query
        self.collection = xent_collection()

    def data_find(self):
        """Uses the db_query from __init__ to find data that we must do something with"""
        cursor = self.collection.find(self.query, {'number': 1, 'data': 1})
        return cursor

    def do_task(self, rundoc):
        """Define in the children classes"""
        raise NotImplementedError

    def single_loop(self):
        cursor = self.data_find()
        for doc in cursor:
            self.do_task(doc)
