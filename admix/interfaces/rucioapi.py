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
import json

class ConfigRucioDataFormat():
    #read/config rucio interface
    def __init__(self):
        self.rucio_configuration_ = None
        self.types_ = []
        self.structure_ = {}
        self.eval_ = 0
        print("Init ConfigRucioDataFormat")
        
    def Config(self, config_path):
        #put in your favourite experimental configuration
        #to describe the rucio cataloge description
        self.config_path = config_path
        self.LoadConfig()
        self.Eval()
        
    def LoadConfig(self):
        with open(self.config_path) as f:
            self.rucio_configuration_ = json.load(f)
    
    def FindOccurrences(self, test_string, test_char):
        #https://stackoverflow.com/questions/13009675/find-all-the-occurrences-of-a-character-in-a-string
        return [i for i, letter in enumerate(test_string) if letter == test_char]
    
    def ExtractTagWords(self, test_string, beg_char, end_char):
        word_list = []
        char_beg = self.FindOccurrences(test_string, "{")
        char_end = self.FindOccurrences(test_string, "}")
        for j in range(len(char_beg)):
            j_char_beg = char_beg[j]
            j_char_end = char_end[j]
            word = test_string[j_char_beg+1:j_char_end]
            if word not in word_list:
                word_list.append(word)
        return word_list
                
    def Eval(self):
        #Get data types (=keys)
        self.types_ = list(self.rucio_configuration_.keys())
        
        if len(self.types_) == 0:
            print("Upload types are not defined")
            
        
        #Get the overall file structure from your configuration file (depends on experiment)
        self.structure_ = {}
        try: 
            for i_type in self.types_:
                i_levels  = self.rucio_configuration_[i_type].split("|->|")
                nb_levels = len(i_levels)
                
                #init the level at least once beforehand
                self.structure_[i_type] = {}
                for idx, i_level in enumerate(i_levels):
                    self.structure_[i_type]["L"+str(idx)] = {}
                
                #fill the levels with information:
                for idx, i_level in enumerate(i_levels):
                    if i_level.find('$C') == 0:
                        self.structure_[i_type]["L"+str(idx)]['type'] = "rucio_container"
                        self.structure_[i_type]["L"+str(idx)]['did'] = i_level.replace("$C", "")
                    if i_level.find('$D') == 0:
                        self.structure_[i_type]["L"+str(idx)]['type'] = "rucio_dataset"
                        self.structure_[i_type]["L"+str(idx)]['did'] = i_level.replace("$D", "")
                    
                    self.structure_[i_type]["L"+str(idx)]['tag_words'] = self.ExtractTagWords(i_level, "{", "}")
            self.eval_ = 1
        except:
            print("Evaluation failed")

    def GetTypes(self):
        return self.types_
    def GetStructure(self):
        return self.structure_

      
        

class RucioCLI():
    
    def __init__(self):
        print("Init the commandline module")
        self.rucio_version = None
        self.rucio_account = None
        self.rucio_host = None

    #Run Bash scripts out of Python:
    def create_script(self, script):
        """Create script as temp file to be run on cluster"""
        fileobj = tempfile.NamedTemporaryFile(delete=False,
                                            suffix='.sh',
                                            mode='wt',
                                            bufsize=1)
                                            #buffering=1)
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

    def SetConfigPath(self, config_path):
        self.rucio_cli_config = config_path
    
    def SetHost(self, host):
        self.rucio_host = host

    def SetProxyTicket(self, ticket_path):
        self.rucio_ticket_path = ticket_path
        
    def SetRucioAccount(self, account):
        self.rucio_account = account
    
    def GetConfig(self):
        config_string = ""
        
        if os.path.isdir(self.rucio_cli_config) and self.rucio_host != None:
            f_file = open(os.path.join(self.rucio_cli_config, 'rucio_cli_{host}.config'.format(host=self.rucio_host)),"r")
            for i_line in f_file:
                #i_line=i_line.replace("\n", "")
                if i_line.find("#!/7")==0:
                    continue
                config_string+=i_line
        else:
            print("Log: Miss config")
        
        return config_string
        
    def ConfigHost(self):
        self.config = self.GetConfig().format(rucio_account=self.rucio_account, x509_user_proxy=self.rucio_ticket_path)
        
class RucioAPI():
    
    def __init__(self):
        self.print_to_screen = False
        self.rucio_enabled = False
        self.rucio_enable_upload_api = False
        
        #init proper rucio session when calling
        #RucioAPI
        try:
            self.rucio_client = Client()
            self.rucio_enabled = True
            
            test_call = self.rucio_client.ping()
            if 'version' in test_call:
                self.rucio_version = test_call['version']
        except:
            print("Can not init the Rucio API")
            print("-> Check for your Rucio installation")
            exit()
        
        #self.rucio_upclient = UploadClient()
        #enable api upload:
        #if version.parse(self.rucio_version) > version.parse("1.14.1"):
        #    self.rucio_enable_upload_api = True
        #print self.rucio_enable_upload_api
        
    def GetRucioVersion(self):
        return self.rucio_version
    
    
    
class TransferRucio(RucioCLI, RucioAPI, ConfigRucioDataFormat):
    def __init__(self):
        print("Init RucioAPI")
        
        
        
        #self.rc = RucioConfig.__init__(self)
        #self.rcdataformat = ConfigRucioDataFormat.__init__(self)