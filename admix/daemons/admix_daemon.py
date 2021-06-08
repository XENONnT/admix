"""
Base class information for different admix daemons
"""
import time
from tqdm import tqdm
from utilix import xent_collection
from admix import logger


class AdmixDaemon:
    query = {}
    projection = {'number': 1}

    def __init__(self, db_query=None):

        if db_query is not None:
            # then we loop over all data
            self.query = db_query

        self.collection = xent_collection()

    def data_find(self, limit=None):
        """Uses the db_query to find data that we must do something with"""
        cursor = self.collection.find(self.query, {'number': 1, 'data': 1}, limit=limit)
        return cursor

    def do_task(self, rundoc):
        """Define in the children classes"""
        raise NotImplementedError

    def single_loop(self, max_iterations=None, progress_bar=True):
        cursor = list(self.data_find(limit=max_iterations))
        logger.info(f"Running {self.__class__.__name__} on {len(cursor)} entries")
        iterable = cursor
        if progress_bar:
            iterable = tqdm(iterable)
        for doc in iterable:
            self.do_task(doc)

    def infinite_loop(self, sleep=600, dt=1, **kwargs):
        try:
            while True:
                self.single_loop(**kwargs)
                # start sleep timer
                total_sleep = sleep
                sleep_left = total_sleep
                while sleep_left > -1:
                    statement = f"Sleeping for {sleep_left} seconds"
                    print(f"{statement:30s}", flush=True, end='\r')
                    time.sleep(dt)
                    sleep_left = sleep_left - dt
        except KeyboardInterrupt:
            print("Exiting.")
