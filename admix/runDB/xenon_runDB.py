import datetime
import logging
import os
import time
import hashlib
import json
import random
import requests
import signal
import socket
import subprocess
import sys
import traceback
import datetime
import io
import locale
import pymongo


class XenonRunDatabase(object):
    
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
    
    
    
    
    #def GetQuery(self):
        #return cursor = collection.find(query)
        
        #"detector":    "tpc",
        #"data": { "$elemMatch": {
##            "host": site,
            #"type": data_type,
            #"status": "transferred"
        #}},
        #"source": { "$in": [ {"type": "AmBe"}, {"type": "Rn220"}]},
        #"tags": {"$elemMatch": { "name": "_sciencerun0" }}
        #}