#from __future__ import with_statement
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
from rucio.client.downloadclient import DownloadClient
from rucio.common.exception import DataIdentifierAlreadyExists

from admix.helper.decorator import Collector
import sys
import tempfile
import subprocess
import datetime
import os
import json

@Collector
class RucioAPI():

    def __init__(self):
        self._print_to_screen = False
        self._rucioAPI_enabled = False
        self._rucio_version = None
        self._rucio_account = None
    def __del__(self):
        pass

    # Here comes the backend configuration part:
    def SetRucioAccount(self, rucio_account=None):
        self._rucio_account = rucio_account
    def SetConfigPath(self, config_path=None):
        pass
    def SetProxyTicket(self, proxy_path=None):
        pass
    def SetHost(self, hostname=None):
        pass
    def ConfigHost(self):
        #This member function setup the rucioAPI backend (load Client())
        try:
            print("ConfigHost, RucioAPI:")
            self._rucio_client = Client()
            self._rucio_client_upload = UploadClient()
            self._rucio_client_download = DownloadClient()

            print("Explore whoami:")
            for key, val in self._rucio_client.whoami().items():
                print(": ", key, "-", val)

            print("Ping:")
            test_call = self._rucio_client.ping()
            print("test version:", test_call)
            if 'version' in test_call:
                self._rucio_version = test_call['version']
            self._rucioAPI_enabled = True
        except:
            print("Can not init the Rucio API")
            print("-> Check for your Rucio installation")
            exit()

    def Alive(self):
        print("RucioAPI alive")
    # finished the backend configuration for the Rucio API

    def GetRucioVersion(self):
        return self._rucio_version

    #The scope section:
    def CreateScope(self, account, scope):
        try:
            self._rucio_client.add_scope(account, scope)
        except:
            print("raises Duplicate: if scope already exists.")
            print("raises AccountNotFound")

        #Several list commands
    def ListContent(self, scope, name):
        try:
            return self._rucio_client.list_content(scope, name)
        except:
            return None

    def ListScopes(self):
        try:
            return self._rucio_client.list_scopes()
        except:
            return None

    def ListFileReplicas(self, scope, name):
        #List file replias for a scope and filename
        try:
            return self._rucio_client.list_file_replicas(scope, name)
        except:
            return None

    def ListFiles(self, scope, name, long=None):
        try:
            return self._rucio_client.list_files(scope, name, long=None)
        except:
            return None

    def ListDidRules(self, scope, name):
        try:
            return self._rucio_client.list_did_rules(scope, name)
        except:
            return None
    #Attach and detach:
    def AttachDids(self, scope, name, attachment, rse=None):
        try:
            self._rucio_client.attach_dids(scope, name, attachment, rse=None)
        except:
            #pass
            print("no attachment done")
            #return None

    def DetachDids(self, scope, name, dids):
        try:
            self._rucio_client.detach_dids(scope, name, dids)
        except:
            return None

    #Container and Dataset managment:
    def CreateContainer(self, scope, name, statuses=None, meta=None, rules=None, lifetime=None):
        try:
            return self._rucio_client.add_container(scope, name, statuses=None, meta=None, rules=None, lifetime=None)
        except DataIdentifierAlreadyExists:
            print("Data identifier already exists for container")
            return None

    def CreateDataset(self, scope, name, statuses=None, meta=None, rules=None, lifetime=None, files=None, rse=None):
        try:
            self._rucio_client.add_dataset(scope, name, statuses=None, meta=None, rules=None, lifetime=None, files=None, rse=None)
        except DataIdentifierAlreadyExists:
            print("Data identifier already exists for dataset")

    #Rules:
    def AddRule(self, dids, copies, rse_expression, weight=None, lifetime=None, grouping='DATASET', account=None,
                locked=False, source_replica_expression=None, activity=None, notify='N', purge_replicas=False,
                ignore_availability=False, comment=None, ask_approval=False, asynchronous=False, priority=3, meta=None):

        try:
            self._rucio_client.add_replication_rule(dids, copies, rse_expression, weight=None, lifetime=lifetime, grouping='DATASET', account=None,
                             locked=False, source_replica_expression=None, activity=None, notify='N', purge_replicas=False,
                             ignore_availability=False, comment=None, ask_approval=False, asynchronous=False, priority=3)
        except:
            print("No rule created for {dids}".format(dids=dids))

    #Metadata:
    def GetMetadata(self, scope, name):
        try:
            return self._rucio_client.get_metadata(scope, name)
        except:
            return None

    def SetMetadata(self, scope, name, key, value, recursive=False):
        try:
            return self._rucio_client.set_metadata(scope, name, key, value, recursive=False)
        except:
            return None

    #Data upload / download / register
    def Upload(self, method=None, upload_dict=None):

        #print(method)
        #print(upload_dict)
        #try:
        #self._rucio_client_upload.upload(upload_dict)
        #except:
        #    print("upload whooopsie")

        #Get your uploads done in the right way:
        if method == 'upload-folder-with-did':
            #The upload option upload-folder-with-did does
            # - Upload all files of a path to a dedicated
            #   scope and dataset name (scope:name)
            # - Upload all files to a the same dedicated scope
            #   such for the dataset (scope)
            # - Attaches the individual files (scope:fileN) to
            #   the dataset (scope:name)
            # The upload_dict needs to be a single dictionary
            # in a list object
            try:
                p = self._rucio_client_upload.upload(upload_dict)
            except:
                p = 1
        elif method == 'upload-folder-with-did-by-file':
            # The upload option upload-folder-with-did does
            # - Upload all files of a path to a dedicated
            #   scope and dataset name (scope:name)
            # - Upload all files to a the same dedicated scope
            #   such for the dataset (scope)
            # - Attaches the individual files (scope:fileN) to
            #   the dataset (scope:name)
            # The upload_dict needs to be a list of dictionaries
            # We loop over it:
            for i_dict in upload_dict:
                self._rucio_client_upload.upload(i_dict)
        else:
            print("Get your upload option right")
        return 1, 1


    def DownloadDids(self, items, num_threads=2, trace_custom_fields={}):

        dw_status = self._rucio_client_download.download_dids(items=items,
                                                       num_threads=num_threads,
                                                       trace_custom_fields=trace_custom_fields)
        return dw_status

    def Register(self, rse, files, ignore_availability=True):
        #See email "IceCube Script to register data"
        #from Benedikt.
        #files = {
                #'scope': self.scope,
                #'name': replicas[filemd]['name'],
                #'adler32': replicas[filemd]['adler32'],
                #'bytes': replicas[filemd]['size'],
               #} for filemd in replicas]
               #--> Think about metadata
        try:
            self._rucio_client.add_replicas(rse, files, ignore_availability)
        except:
            print("Problem with file name does not match pattern")


        for filemd in replicas:
            try:
                self.didc.attach_dids(scope=self.scope, name=self.run_Number, dids=[{
                    'scope': self.scope,
                    'name': replicas[filemd]['name']}])
            except FileAlreadyExists:
                print("File already attached")
