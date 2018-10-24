import pymongo
import requests
import http.client
from admix.tasks import helper

class ConnectMongoDB():
    
    def __init__(self):
        self.db_mongodb_string = helper.get_hostconfig()['database']['address']
        self.db_mongodb_pw     = helper.get_hostconfig()['database']['password']
        
        if 'collection' not in helper.get_hostconfig()['database']:
            self.collection_string = []
            print("Define a collection in your configuration first (separated by -)")
            exit()
        else:
            try:
                self.collection_string = helper.get_hostconfig()['database']['collection'].split("-")
            except:
                self.collection_string = helper.get_hostconfig()['database']['collection']
        
        if 'projection' not in helper.get_hostconfig()['database']:
            self.projection = None
        else:
            self.projection = helper.get_hostconfig()['database']['projection']

            
    def Connect(self):
        c = pymongo.MongoClient(self.db_mongodb_string)
        
        self.db = c
        for i_str in range(len(self.collection_string)):
            self.db = self.db[self.collection_string[i_str]]
        
    def GetRunByNumber(self, run_number, reconnect=False):
        
        if reconnect == True:
            self.Connect()
        
        query = {"number": run_number}
        return list(self.db.find(query, projection=self.projection ))

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
        
class DataBase(ConnectMongoDB, RestAPI):
    
    def __init__(self):
        print("I am your database")
        db_type     = helper.get_hostconfig()['database']['type']
        if db_type == "MongoDB":
            self.dbc = ConnectMongoDB()
            self.dbc.Connect()
            run_id = self.dbc.GetRunByNumber(13000)
            for i in run_id[0]['data']:
                print( i['host'], i['type'] )
        
        if db_type == "RestAPI":
            self.dbc = RestAPI()
            self.dbc.ShowSitemap()
            
        
        
        