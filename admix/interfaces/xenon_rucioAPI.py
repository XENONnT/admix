#This will be the frontend for Rucio

from __future__ import with_statement
import rucio
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
from rucio.common.exception import DataIdentifierAlreadyExists

import sys
import tempfile
import subprocess
import datetime
import os

class RucioReturnsNothing():
    pass

class RucioAPI(object):
    
    def __init__(self):
        self.print_to_screen = False
        
        #init proper rucio session when calling
        #RucioAPI
        try:
            self.rucio_client = Client()
            self.rucio_upclient = UploadClient(self.rucio_client)
        except:
            print("Can not init the Rucio API")
            print("-> Check for your Rucio installation")
            exit()
    
    def SetLogger(self, set):
        self.print_to_screen = set
        
    def logger(self, log_string):
        if self.print_to_screen == True:
            print(log_string)
    
    #Run Bash scripts out of Python:
    def create_script(self, script):
        """Create script as temp file to be run on cluster"""
        fileobj = tempfile.NamedTemporaryFile(delete=False,
                                            suffix='.sh',
                                            mode='wt',
                                            buffering=1)
        fileobj.write(script)
        os.chmod(fileobj.name, 0o774)
        return fileobj
    
    def delete_script(self, fileobj):
        """Delete script after submitting to cluster
        :param script_path: path to the script to be removed
        """
        fileobj.close()
    
    def doRucio(self, upload_string ):
        sc = self.create_script( upload_string )    
        execute = subprocess.Popen( ['sh', sc.name] , 
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT,
                                    shell=False,
                                    universal_newlines=False)
        stdout_value, stderr_value = execute.communicate()
        stdout_value = stdout_value.decode("utf-8")
        stdout_value = stdout_value.split("\n")
        stdout_value = list(filter(None, stdout_value)) # fastest way to remove '' from list
        self.delete_script(sc)
        return stdout_value, stderr_value
    
    
    #Init Rucio access by class:
    def InitProxy(self, host=None, cert_path=None, key_path=None, ticket_path=None):
        
        str_midway = """
#.bashrc
source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh
voms-proxy-init -voms xenon.biggrid.nl -cert {cert_path} -key {key_path} -valid 168:00 -out {ticket_path}
echo "A new proxy-init for xenon.biggrid.nl is requested (168h)"
echo "path: {ticket_path}"
"""

        #Select hosts:
        exec_string=None
        if host=="midway" or host=="midway2":
            exec_string = str_midway.format(cert_path=cert_path, key_path=key_path, ticket_path=ticket_path)
            
        #Execute hosts:
        if host==None or cert_path==None or key_path==None or ticket_path==None:
            return -1
        else:
            msg, err = self.doRucio(exec_string)
            
            is_done=False
            for i in msg:
                if i.find("Creating proxy")>=0 and i.find("Done") >= 0:
                    is_done=True
                if is_done == True and i.find("Your proxy is valid until")>=0:
                    expire_date = list(filter(None, i.replace("Your proxy is valid until", "").split(" ")))
                    if len(expire_date[2]) < 2: expire_date[2]="0%s"%expire_date[2]
                    str_expire_date = "{Y}-{M}-{day}-{time}".format(Y=expire_date[4], M=expire_date[2], day=expire_date[1], time=expire_date[3])
                    str_expire_date = datetime.datetime.strptime(str_expire_date, "%Y-%d-%b-%H:%M:%S")
                    self.logger("InitProxy successfull for {host}".format(host=host))
                    self.logger("Ticket expires at {date}".format(date=str_expire_date))
    
    def whoami(self):
        print(self.rucio_client.whoami())
    
    #The scope section:
    def CreateScope(self, account, scope):
        try:
            self.rucio_client.add_scope(account, scope)
        except RucioReturnsNothing:
            print("raises Duplicate: if scope already exists.")
            print("raises AccountNotFound")
    
    
    #Several list commands
    def ListScopes(self):
        try:
            return self.rucio_client.list_scopes()
        except RucioReturnsNothing:
            return None
    
    def ListFileReplicas(self, scope, fname):
        #List file replias for a scope and filename
        try:
            return self.rucio_client.list_file_replicas(scope, dname)
        except:
            return None
        
    def ListFiles(self, scope, name, long=None):
        try:
            return self.rucio_client.list_files(scope, name, long=None)
        except:
            return None
        
    #Attach and detach:
    def AttachDids(self, scope, name, dids, rse=None):
        try:
            self.rucio_client.attach_dids(scope, name, dids, rse=None)
        except:
            return None
    
    def DetachDids(self, scope, name, dids):
        try:
            self.rucio_client.detach_dids(scope, name, dids)
        except:
            return None
    
    #Container and Dataset managment:
    def CreateContainer(self, scope, name, statuses=None, meta=None, rules=None, lifetime=None):
        try:
            return self.rucio_client.add_container(scope, name, statuses=None, meta=None, rules=None, lifetime=None)
        except DataIdentifierAlreadyExists:
            print("Data identifier already exists for container")
            return None
    
    def CreateDataset(self, scope, name, statuses=None, meta=None, rules=None, lifetime=None, files=None, rse=None):
        try:
            self.rucio_client.add_dataset(scope, name, statuses=None, meta=None, rules=None, lifetime=None, files=None, rse=None)
        except DataIdentifierAlreadyExists:
            print("Data identifier already exists for dataset")
    
    #Metadata:
    def GetMetadata(self, scope, name):
        try:
            return self.rucio_client.get_metadata(scope, name)
        except:
            return None
    
    def SetMetadata(self, scope, name, key, value, recursive=False):
        try:
            return self.rucio_client.set_metadata(scope, name, key, value, recursive=False)
        except:
            return None
    
    #Data upload / download / register
    def Upload(self, items, summary_file_path=None):
        self.rucio_upclient.upload(items, summary_file_path)
        print("T")
        
        
    def Download(self):
        pass
    
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
            self.rucio_client.add_replicas(rse, files, ignore_availability)
        except:
            print("Problem with file name does not match pattern")


        for filemd in replicas:
            try:
                self.didc.attach_dids(scope=self.scope, name=self.run_Number, dids=[{
                    'scope': self.scope,
                    'name': replicas[filemd]['name']}])
            except FileAlreadyExists:
                print("File already attached")
        
        
        
        
        
        
        
        
        