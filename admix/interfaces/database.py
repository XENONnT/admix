import pymongo
import requests
import http.client
from admix.tasks import helper

class ConnectMongoDB():

    def __init__(self):
        self.db_mongodb_string = helper.get_hostconfig()['database']['address']
        self.db_mongodb_pw     = helper.get_hostconfig()['database']['password']
        self.db = None

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

    def GetSmallest(self, key):
        return sorted(list(self.db.find({}, projection={key: True, '_id':True})), key=lambda k: k[key])[0][key]

    def GetLargest(self, key):
        return sorted(list(self.db.find({}, projection={key: True, '_id':True})), key=lambda k: k[key])[-1][key]

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
