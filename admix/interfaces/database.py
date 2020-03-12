import http.client

import pymongo
import requests

import admix.helper.helper as helper


class ConnectMongoDB():

    def __init__(self):
        self.db_mongodb_address = helper.get_hostconfig()['database']['address']
        self.db_mongodb_user = helper.get_hostconfig()['database']['user']
        self.db_mongodb_pw     = helper.get_hostconfig()['database']['password']
        self.db_mongodb_string = None
        self.db = None

        #bulid the mongodb address string:
        self._make_connection_string()

        if 'collection' not in helper.get_hostconfig()['database']:
            self.collection_string = []
            print("Define a collection in your configuration first (separated by -)")
            exit()
        else:
            try:
                self.collection_string = helper.get_hostconfig()['database']['collection'].split("-")
            except:
                self.collection_string = helper.get_hostconfig()['database']['collection']

        #Define the basic projections for mongoDB
        self.SetProjection(projection=None, from_config=True)
        # make connection to runDB
        self.Connect()

    def _make_connection_string(self):
        if "mongodb://" not in self.db_mongodb_address:
            print("Not a mongoDB address!")
        else:
            taddress = self.db_mongodb_address.replace("mongodb://", "")
            self.db_mongodb_string = f'mongodb://{self.db_mongodb_user}:{self.db_mongodb_pw}@{taddress}'

    def SetProjection(self, projection=None, from_config=False):

        if from_config == True:
            if 'projection' not in helper.get_hostconfig()['database']:
                self.projection = None
            else:
                self.projection = helper.get_hostconfig()['database']['projection']
        else:
            self.projection = projection


    def Connect(self):
        c = pymongo.MongoClient(self.db_mongodb_string)

        self.db = c
        for i_str in range(len(self.collection_string)):
            self.db = self.db[self.collection_string[i_str]]

    def GetQuery(self, query, reconnect=False, sort=[('_id',1)]):
        if reconnect == True:
            self.Connect()

        return list(self.db.find(query, projection=self.projection).sort(sort) )


    def GetRunByNumber(self, run_number, reconnect=False):

        if reconnect == True:
            self.Connect()

        query = {"number": run_number}
        return list(self.db.find(query, projection=self.projection ))


    def GetRunByName(self, run_name, reconnect=False):
        if reconnect == True:
            self.Connect()

        query = {"name": run_name}
        return list(self.db.find(query, projection=self.projection ))

    def GetRunByNameNumber(self, run_name, run_number, reconnect=False):
        if reconnect == True:
            self.Connect()

        query = { "$and":[ {"name": run_name}, {"number":run_number}]}
        return list(self.db.find(query, projection=self.projection ))

    def GetRunByID(self, run_id, reconnect=False):
        if reconnect == True:
            self.Connect()

        query = {"_id": run_id}
        return list(self.db.find(query, projection=self.projection ))

    def GetRunsByTimestamp(self, ts_beg=None, ts_end=None, reconnect=False, sort=[('_id',1)]):
        if reconnect == True:
            self.Connect()

        if ts_beg == None:
            ts_beg = self.GetSmallest("start")
        if ts_end == None:
            ts_end = self.GetLargest("start")

        query = { '$and': [ {'start': {'$gte':ts_beg}}, {'start': {'$lte':ts_end}} ] }
        projection = {"name":True, "number": True, "start":True, "_id":True}
        return list(self.db.find(query, projection=projection).sort(sort) )

    def GetSmallest(self, key="number"):
        return sorted(list(self.db.find({}, projection={"name": True, "number":True, "_id":True})), key=lambda k: k["number"])[0].get(key)

    def GetLargest(self, key="number"):
        return sorted(list(self.db.find({}, projection={"name": True, "number":True, "_id":True})), key=lambda k: k["number"])[-1].get(key)
        #return sorted(list(self.db.find({}, projection={key: True, '_id':True})), key=lambda k: k[key])[-1][key]

    def GetBoundary(self, key="number"):
        #Evaluate the full run database for the first and last element and return
        #a dictionary with its information. If None, then db entry does not exists
        klist = sorted(list(self.db.find({}, projection={"name": True, "number":True, "start": True, "_id":True})), key=lambda k: k["number"])
        kdict = {"min_name": klist[0].get('name'),
                 "min_number": klist[0].get('number'),
                 "min_start_time": klist[0].get('start'),
                 "max_name": klist[-1].get('name'),
                 "max_number": klist[-1].get('number'),
                 "max_start_time": klist[-1].get('start'),
                 }
        return kdict


    def GetRunsByTag(self, tag=None, sort="ascending"):

        #check if tag is a list or a string:
        if type(tag) == str:
            tag = [tag]
        elif type(tag) != list or tag==None:
            return {}

        #create an aggregation from the list of tags:
        tag_name_match = [ {"tags.name": i_tag} for i_tag in tag]
        tag_name_match = {"$match": {"$and": tag_name_match}}

        #evaluate sort direction:
        if sort == "ascending":
            sort = -1 #ascending means from latest to earliest run
        else:
            sort = 1

        agr = [
            {"$project":  {"tags": True, "name": True, "number": True, "start":True}},
            tag_name_match,
            {"$sort": {"start": sort}}
            ]
        return list(self.db.aggregate(agr))

    def GetRunsBySource(self, source=None, sort="ascending"):

        #check if source is a list or a string:
        if type(source) == str:
            source = [source]
        elif type(source) != list or source==None:
            return {}

        #create an aggregation from the list of tags:
        source_name_match = [ {"source.type": i_source} for i_source in source]
        source_name_match = {"$match": {"$and": source_name_match}}

        #evaluate sort direction:
        if sort == "ascending":
            sort = -1 #ascending means from latest to earliest run
        else:
            sort = 1

        agr = [
            {"$project":  {"name": True, "number": True, "start":True, "source":True}},
            source_name_match,
            {"$sort": {"start": sort}}
            ]
        return list(self.db.aggregate(agr))

    def FindTimeStamp(self, key, value):
        #timestamp definition:
        # The timestamp is defined according the 'start' tag in the runDB:
        runDB_tag_timestamp = 'start'
        # <Hard coded Change later>
        query = {key:value}
        return list(self.db.find(query, projection={key: True, '_id':True, runDB_tag_timestamp:True}))[0][runDB_tag_timestamp]

    def SetStatus(self, id_field, type=None, host=None, status=None):
        #This function is database entry specific:
        #It changes a list (data) which contains dictionaries
        #In particular you can change here the status field
        run = self.GetRunByID(id_field)[0]

        old_data = run['data']
        new_data = old_data.copy()

        if type != None and host!=None and status != None:
            for i_run in new_data:
                if i_run['type'] != type:
                    continue
                if i_run['host'] != host:
                    continue
                if status != None:
                    i_run['status'] = status

        self.db.find_one_and_update({"_id": id_field},
                                        {"$set": {"data": new_data}})

    def SetDestination(self, id_field, type=None, host=None, destination=None):
        #This function is database entry specific:
        #It changes a list (data) which contains dictionaries
        #In particular you can change here the destination field
        run = self.GetRunByID(id_field)[0]

        old_data = run['data']
        new_data = old_data.copy()

        if type != None and host!=None and destination != None:
            for i_run in new_data:
                if i_run['type'] != type:
                    continue
                if i_run['host'] != host:
                    continue
                if destination != None:
                    i_run['destination'] = destination

        self.db.find_one_and_update({"_id": id_field},
                                        {"$set": {"data": new_data}})

    def GetDataField(self, id_field, type=None, host=None,
                     key=None):

        run = self.GetRunByID(id_field)[0]

        value = "dict_not_exists"
        for i_run in run['data']:
            if i_run['type'] != type:
                continue
            if i_run['host'] != host:
                continue
            if key not in i_run:
                value="key_not_exists"
            elif key in i_run:
                value=i_run[key]
        #if value==dict_not_exists: given host/type combination in the dictionary does not exits
        #elif: key not in dict: key_not_exists
        #elif: return key of dict
        return value

    def GetDestination(self, ts_beg=None, ts_end=None, sort="ascending"):

        if ts_beg == None:
            ts_beg = self.GetSmallest("start")
        if ts_end == None:
            ts_end = self.GetLargest("start")

        #evaluate sort direction:
        if sort == "ascending":
            sort = -1 #ascending means from latest to earliest run
        else:
            sort = 1

        col = self.db.aggregate(
                    [
                        {"$match": {'$and': [ {'start': {'$gte':ts_beg}}, {'start': {'$lte':ts_end}} ]}},
                        {"$unwind": "$data"},
                        {"$project": {
                                        "name": 1, "number": 1, "destination": "$data.destination",
                                        "ndest": { "$cond": [{ "$isArray": "$data.destination"}, {"$size": "$data.destination"}, 0]},
                                    }},
                        {"$match": {"ndest": {"$gt": 0}}},
                        {"$project": {"name":1, "number":1}},
                        {"$sort": {"start": sort}}

                    ]
                )

        return col

    def GetHosts(self, host, ts_beg=None, ts_end=None, sort="ascending"):
        """Function: GetLocations
        Get a selection of events between two timestamps (ts_beg, ts_end) for a pre-selected host
        in the meta database.
        :param host: A general location search to look for results of a specific host in the meta database
        :param ts_beg: datetime object when to begin the search
        :param ts_end: datetime object when to end the search
        :param sort: How to sort the result: ascending or descending
        :return MongoDB aggregation:
        """

        if ts_beg == None:
            ts_beg = self.GetSmallest("start")
        if ts_end == None:
            ts_end = self.GetLargest("start")

        #evaluate sort direction:
        if sort == "ascending":
            sort = -1 #ascending means from latest to earliest run
        else:
            sort = 1

        col = self.db.aggregate(
                    [
                        {"$match": {'$and': [{'start': {'$gte':ts_beg}}, {'start': {'$lte':ts_end}}]}},
                        {"$unwind": "$data"},
                        {"$project": {
                                        "name": 1, "number": 1, "host": "$data.host",
                                    }},
                        {"$match": {"host": host}},
                        {"$project": {"name":1, "number":1}},
                        {"$sort": {"start": sort}}
                    ]
                )

        return col

    def GetClearance(self, ts_beg=None, ts_end=None, sort="ascending"):
        """Function: GetClearance(...)

        Returns from a mongoDB database all entries which have a list of dictionaries (data field) and an individual
        data dictionary contains a key "status" with the value "RucioClearance"

        :param ts_beg: A datetime object to set a time interval - Start value (field 'start')
        :param ts_end: A datetime object to set a time interval - Stop value (field 'start')
        :param sort: Sort your mongodb aggregation ascending (standard) or descending
        :return collection: The summary collection of the requested status (RucioClearance) with name and number
                            field only. Acts as reduced summary
        """
        if ts_beg == None:
            ts_beg = self.GetSmallest("start")
        if ts_end == None:
            ts_end = self.GetLargest("start")

        #evaluate sort direction:
        if sort == "ascending":
            sort = -1 #ascending means from latest to earliest run
        else:
            sort = 1

        collection = self.db.aggregate(
                    [
                        {"$match": {'$and': [ {'start': {'$gte':ts_beg}}, {'start': {'$lte':ts_end}} ]}},
                        {"$unwind": "$data"},
                        {"$project": {"name":1, "number":1, "status": "$data.status"}},
                        {"$match": {"status": "RucioClearance"}},
                        {"$project": {"name":1, "number":1, "data":1}},
                        {"$sort": {"start": sort}}
                    ]
                )

        return collection

    def StatusDataField(self, id_field, type=None, host=None):
        #Test if a dictionary exists in a list for a specific combination of type and host
        run = self.GetRunByID(id_field)[0]

        value = False
        for i_run in run['data']:
            if i_run['type'] != type:
                continue
            if i_run['host'] != host:
                continue
            value=True

        return value

    def ShowDataField(self, id_field, type=None, host=None):
        #Test if a dictionary exists in a list for a specific combination of type and host
        run = self.GetRunByID(id_field)[0]

        for i_run in run['data']:
            if i_run['type'] != type:
                continue
            if i_run['host'] != host:
                continue
            print(" [-> ", i_run['type'], "/", i_run['host'], " <-]")
            if 'rse' in i_run:
                print(" [-> ", i_run['rse'])
            else:
                print(" [->  No RSE information")
            if 'status' in i_run:
                print(" [-> ", i_run['status'])
            else:
                print(" [->  No status information")
            if 'meta' in i_run:
                print(" [-> ", i_run['meta'])
            else:
                print(" [->  No meta information")

    def SetDataField(self, id_field, type=None, host=None,
                     key=None, value=None, new=False):
        #This function is database entry specific:
        #It changes a list (data) which contains dictionaries
        #In particular you can change here the destination field
        run = self.GetRunByID(id_field)[0]

        new_data = run['data']
        if type != None and host!=None and key != None:
            for i_run in new_data:
                if i_run['type'] != type:
                    continue
                if i_run['host'] != host:
                    continue
                if i_run['type'] == type and i_run['host'] == host and key in i_run:
                    i_run[key] = value
                if i_run['type'] == type and i_run['host'] == host and new==True:
                    i_run[key] = value

        self.db.find_one_and_update({"_id": id_field},
                                        {"$set": {"data": new_data}})

    def AddDatafield(self, id_field, new_dict):
        run = self.GetRunByID(id_field)[0]

        old_data = run['data']
        old_data.append(new_dict)

        #print("NEW", old_data)
        self.db.find_one_and_update({"_id": id_field},
                                        {"$set": {"data": old_data}})

    def RemoveDatafield(self, id_field, rem_dict):
        run = self.GetRunByID(id_field)[0]

        old_data = run['data']

        for i_d in old_data:
            if i_d == rem_dict:
                print(i_d['host'], i_d['location'], )
                index_tmp = old_data.index(rem_dict)
                del(old_data[index_tmp])

        self.db.find_one_and_update({"_id": id_field},
                                        {"$set": {"data": old_data}})

    def UpdateDatafield(self, id_field, new_data=None):
        run = self.GetRunByID(id_field)[0]

        old_data = run['data']

        if old_data==new_data:
            return 0
        else:
            list_diff = [x for x in new_data if x not in old_data]

            for i in list_diff:
                print("New destination for host {host}:".format(host=i['host']))
                print(" -> {dest}".format(dest=i['destination']))
            self.db.find_one_and_update({"_id": id_field},
                                        {"$set": {"data": new_data}})


    def WriteDestination(self, id_field, type=None, host=None, destination=None):
        run = self.GetRunByID(id_field)[0]

        new_data = run['data']

        for i_run in new_data:
            if 'destination' not in i_run:
                i_run['destination']= []

        if type != None and host!=None and destination != None:
            for i_run in new_data:
                if i_run['type'] != type:
                    continue
                if i_run['host'] != host:
                    continue
                if destination in i_run['destination']:
                    break
                print("Change a destination for:", type, "and", host, "to", destination)
                i_run['destination'].append(destination)

            print("The new destination is")
            for i_run in new_data:
                print(" <> ", i_run['host'], i_run['location'], i_run['destination'])


            self.db.find_one_and_update({"_id": id_field},
                                        {"$set": {"data": new_data}})

            print("after new set (test)")
            #run = self.GetRunByID(id_field)[0]
            #for i_run in run['data']:
                #print(" <new> -", i_run['host'], i_run['location'], i_run['destination'])

class RestAPI():

    def __init__(self):
        self.db_restapi_string = helper.get_hostconfig()['database']['address']
        self.db_restapi_token  = helper.get_hostconfig()['database']['token']

    def Request(self, thing):
        try:
            conn = http.client.HTTPConnection(self.db_restapi_string)

            headers = {
                    'Content-Type': "application/json",
                    'cache-control': "no-cache",
                    'Authorization': "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE1NDAzMTIzNzEsImV4cCI6MTU0MDM5ODc3MSwicmZfZXhwIjoxNTQyOTA0MzcxLCJqdGkiOiI5MTBhMTNjOC05YzU0LTRhNzMtOTg1My04MzgyM2E4YTBiZDciLCJpZCI6MiwicmxzIjoiYWRtaW4scHJvZHVjdGlvbix1c2VyIn0.xgaDNGKukAd8bCBSjQnsT8Ss1y7vCImBLPMuqWOO52E"
                    }

            conn.request("GET", "run,number,10144,data,dids,", headers=headers)

            res = conn.getresponse()
            data = res.read()
            print(data)

            #r = requests.get(self.db_restapi_string)
            return r
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            return None

    def ShowSitemap(self):
        print("sitemap")
        print(self.db_restapi_string)
        r = self.Request("sitemap")
        print(r)

    def GetRunByNumber(self, run_number):
        return 2

    def GetQuery(self, query):
        return 3

#def DatabaseStyle(f):
    #def Select(f):
        #print("f in")
        #f.Run()
        #print("f out")
    #return Select


class DataBase(ConnectMongoDB, RestAPI):
    def __init__(self):
        print("I am your database")
        self.db_type     = helper.get_hostconfig()['database']['type']

        if self.db_type == "MongoDB":
            self.cdb = ConnectMongoDB()
            self.cdb.Connect()
        elif self.db_type == "RestAPI":
            self.cdb = RestAPI()


    def GetRunByNumber(self, number):
        return self.cdb.GetRunByNumber(number)

    def GetQuery(self, query):
        return self.cdb.GetQuery(query)
