import os
import datetime
import pymongo
from .api import api

class XenonBaseDB:
    '''Base class for runDB'''
    def __init__(self):
        self.collection = None
        self.init_collection()

    def init_collection(self):
        raise NotImplementedError

    def query(self, q, projection=None):
        raise NotImplementedError

    # TODO add some standard queries
    # TODO add some write functions

class XenonPymongoDB(XenonBaseDB):
    '''Pymongo interface to the runDB'''
    def init_collection(self):
        uri = 'mongodb://pax:%s@xenon1t-daq.lngs.infn.it:27017,copslx50.fysik.su.se:27017,zenigata.uchicago.edu:27017/run'
        uri = uri % os.environ.get('MONGO_PASSWORD')
        c = pymongo.MongoClient(uri,
                                replicaSet='runs',
                                readPreference='secondaryPreferred')
        db = c['run']
        self.collection = db['runs_new']

    def query(self, q, projection=None):
        return self.collection.find(q, projection=projection)


class XenonRestDB(XenonBaseDB):
    '''API intergace to the runDB'''
    def init_collection(self):
        self.collection = api()

    def query(self, q, projection=None):
        cursor = self.collection.get_all_runs(q)
        if projection is not None:
            new_cursor = []
            for run in cursor:
                projection['_id'] = projection.get('_id', 1)
                new_cursor.append({key:run[key] for key, b in projection.items() if b})
            cursor = new_cursor
            del new_cursor
        return cursor


class XenonRunDatabase:
    def __init__(self):
        self.collection = None
        self.q_run_number = None
        self.q_run_name   = None
        self.q_timestamp  = None

    def LoadCollection(self):
        uri = 'mongodb://eb:%s@xenon1t-daq.lngs.infn.it:27017,copslx50.fysik.su.se:27017,zenigata.uchicago.edu:27017/run'
        uri = uri % os.environ.get('MONGO_PASSWORD')
        c = pymongo.MongoClient(uri,
                                replicaSet='runs',
                                readPreference='secondaryPreferred')

        db = c['run']
        self.collection = db['runs_new']


    def QueryByRunnumber(self, run_number=None):
        self.q_run_number = run_number

    def QueryByRunname(self, run_name=None):
        self.q_run_name = run_name

    def QueryByTimestamp(self, timestamp=None):
        self.q_timestamp=timestamp

    def CreateQuery(self):

        # This is the query
        self.query = {}
        query_collector = []
        if self.q_run_number != None:
            number_cond = {}
            number_conditions = []
            for i_rn in self.q_run_number:
                qy = { 'number' : {"$eq": int(i_rn)}}
                number_conditions.append( qy )
            number_cond['$or'] = number_conditions
            query_collector.append(number_cond)

        if self.q_run_name != None:
            name_cond = {}
            name_conditions = []
            for i_rn in self.q_run_name:
                qy = { 'name' : {"$eq": str(i_rn)}}
                name_conditions.append( qy )
            name_cond['$or'] = name_conditions
            query_collector.append(name_cond)

        if self.q_timestamp != None:
            timestamp_cond = {}
            timestamp_conditions = []
            for i_ts in self.q_timestamp:
                i_ts = i_ts.split("-")
                beg_i_ts = datetime.datetime.strptime(i_ts[0], '%y%m%d_%H%M')
                end_i_ts = datetime.datetime.strptime(i_ts[1], '%y%m%d_%H%M')
                qy = { '$and': [ { "start": { "$gte": beg_i_ts } }, { "start": { '$lte': end_i_ts } } ] }
                timestamp_conditions.append( qy )
            timestamp_cond['$or'] = timestamp_conditions
            query_collector.append(timestamp_cond)

        #Create the final query for mongoDB:
        if len(query_collector) > 0:
            self.query = { '$or': query_collector }

    def GetQuery(self):
        return self.query

    def GetCollection(self):
        return self.collection

    def GetCursor(self):
        #ToDo: Separate this into a function to set from teh outside later
        sort_key = (('start', -1),
                    ('number', -1),
                    ('detector', -1),
                    ('_id', -1))

        projection = {"detector": True,
                     "number": True,
                     "data": True,
                     "_id": True,
                     "tags" : True,
                     "name": True
                     }

        self.cursor = list(self.collection.find(self.query,
                                                projection=projection,
                                                sort=sort_key))

        return self.cursor
