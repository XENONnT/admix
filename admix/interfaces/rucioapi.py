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

        self.print_to_screen = False
        self.rucioCLI_enabled = False

    #Run Bash scripts out of Python:
    def create_script(self, script):
        """Create script as temp file to be run on cluster"""
        fileobj = tempfile.NamedTemporaryFile(delete=False,
                                            suffix='.sh',
                                            mode='wt',
                                            #bufsize=1)
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
        try:
            self.config = self.GetConfig().format(rucio_account=self.rucio_account, x509_user_proxy=self.rucio_ticket_path)
            self.rucioCLI_enabled = True
        except:
            self.config = ""
            self.rucioCLI_enabled = False
            
    def Whoami(self):
        
        upload_string = "rucio whoami"
        upload_string = self.config + upload_string
        msg, err = self.doRucio(upload_string)
        for i_msg in msg:
            print( i_msg )
    
    def CliUpload(self, method=None, upload_dict={}):
        
        upload_string = ""
        if method == "upload-folder-with-did":
            upload_string = "rucio upload --rse {rse} --scope {scope} {scope}:{did} {datasetpath}\n".format(rse=upload_dict['rse'],scope=upload_dict['scope'],did=upload_dict['did'], datasetpath=upload_dict['upload_path'])
            
        #cliupload:
        upload_string = self.config + upload_string
        
        print(upload_string)
        msg, err = self.doRucio(upload_string)
        for i_msg in msg:
            print( i_msg)                
        
class RucioAPI():
    
    def __init__(self):
        self.print_to_screen = False
        self.rucioAPI_enabled = False
        self.rucio_enable_upload_api = False
        
        #init proper rucio session when calling
        #RucioAPI
        try:
            self.rucio_client = Client()
            self.rucioAPI_enabled = True
            
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
    
    #The scope section:
    def CreateScope(self, account, scope):
        try:
            self.rucio_client.add_scope(account, scope)
        except:
            print("raises Duplicate: if scope already exists.")
            print("raises AccountNotFound")
            
        #Several list commands
    def ListContent(self, scope, name):
        try:
            return self.rucio_client.list_content(scope, name)
        except:
            return None
        
    def ListScopes(self):
        try:
            return self.rucio_client.list_scopes()
        except:
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
    
    def ListDidRules(self, scope, name):
        try:
            return self.rucio_client.list_did_rules(scope, name)
        except:
            return None
    #Attach and detach:
    def AttachDids(self, scope, name, attachment, rse=None):
        try:
            self.rucio_client.attach_dids(scope, name, attachment, rse=None)
        except:
            #pass
            print("no attachment done")
            #return None
    
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
    
    #Rules:
    def AddRule(self, dids, copies, rse_expression, weight=None, lifetime=None, grouping='DATASET', account=None,
                locked=False, source_replica_expression=None, activity=None, notify='N', purge_replicas=False,
                ignore_availability=False, comment=None, ask_approval=False, asynchronous=False, priority=3, meta=None):
        
        try:
            self.rucio_client.add_replication_rule(dids, copies, rse_expression, weight=None, lifetime=None, grouping='DATASET', account=None,
                             locked=False, source_replica_expression=None, activity=None, notify='N', purge_replicas=False,
                             ignore_availability=False, comment=None, ask_approval=False, asynchronous=False, priority=3)
        except:
            print("No rule created for {dids}".format(dids=dids))
    
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
    
    
class TransferRucio(RucioCLI, RucioAPI, ConfigRucioDataFormat):
    def __init__(self):
        print("Init RucioAPI")
        self.rc_cli = RucioCLI()
        self.rc_api = RucioAPI()
    
    def SetAccount(self, account):
        self.rucio_account = account
    
    def init(self):
        print("Enable Rucio CLI:", self.rc_cli.rucioCLI_enabled)
        if self.rc_cli.rucioCLI_enabled == False:
            print("Check your command line configuration")
        print("Enable Rucio API:", self.rc_api.rucioAPI_enabled)
    
    def VerifyStructure(self, upload_structure=None):
        
        sorted_keys = [key for key in sorted(upload_structure.keys())]
        for i_key in sorted_keys:
            
            #assume: We want to check only for the lowest container or dataset:
            val_scope = upload_structure[i_key]['did'].split(":")[0]
            val_dname = upload_structure[i_key]['did'].split(":")[1]
            list_files_rucio = self.rc_api.ListContent(val_scope, val_dname)
            for i_rucio in list_files_rucio:
                print(i_rucio['type'])
        print(upload_structure)        
    
    def AddRules(self, upload_structure=None, rse_rules=None):
        
        #sort locations again
        sorted_keys = [key for key in sorted(upload_structure.keys())]
        
        #assume: We want to check only for the lowest container or dataset:
        val_scope = upload_structure[sorted_keys[-1]]['did'].split(":")[0]
        val_dname = upload_structure[sorted_keys[-1]]['did'].split(":")[1]
        
        #Get Rules:
        r_rules = self.rc_api.ListDidRules(val_scope, val_dname)
        r_rse_list = []
        for i_rule in r_rules:
            r_rse = i_rule['rse_expression']
            r_rse_list.append(r_rse)
        
        for i_rule in rse_rules:
            g_ptr = i_rule.split(":")[0]
            g_rse = i_rule.split(":")[1]
            g_rlt = i_rule.split(":")[2]
            if g_rlt == "None":
                g_rlt = None
            else:
                g_rlt = int(g_rlt)
            if g_rse in r_rse_list:
                continue
            
            print("Create", g_ptr, g_rse, g_rlt)
            dids = {}
            dids['scope'] = val_scope
            dids['name']  = val_dname
            self.rc_api.AddRule( [dids],
                                 copies=1,
                                 rse_expression=g_rse,
                                 lifetime=g_rlt)
    
    def GetRule(self, upload_structure=None, rse=None):
        self.init()
        
        #This function checks if for a given upload structure and a requested
        #upload destination already a rule exists in Rucio
        
        #sort locations again
        sorted_keys = [key for key in sorted(upload_structure.keys())]
        
        #assume: We want to check only for the lowest container or dataset:
        val_scope = upload_structure[sorted_keys[-1]]['did'].split(":")[0]
        val_dname = upload_structure[sorted_keys[-1]]['did'].split(":")[1]
        
        rule_exists = None
        r_rules = self.rc_api.ListDidRules(val_scope, val_dname)
        for i_rule in r_rules:
            if i_rule['rse_expression'] == rse:
                rule_exists = i_rule
        return rule_exists
    
    def CheckRule(self, upload_structure=None, rse=None):
        self.init()
        
        #This function checks if for a given upload structure and a requested
        #upload destination already a rule exists in Rucio
        
        #sort locations again
        sorted_keys = [key for key in sorted(upload_structure.keys())]
        
        #assume: We want to check only for the lowest container or dataset:
        val_scope = upload_structure[sorted_keys[-1]]['did'].split(":")[0]
        val_dname = upload_structure[sorted_keys[-1]]['did'].split(":")[1]
        
        rule_exists = False
        r_rules = self.rc_api.ListDidRules(val_scope, val_dname)
        for i_rule in r_rules:
            if i_rule['rse_expression'] == rse:
                exists = True
        return rule_exists
        
    
    def VerifyLocations(self, upload_structure=None, upload_path=None, checksum_test=False):
        self.init()
        
        #print(upload_path)
        #print(upload_structure)
        
        #get all files from the physical location:
        list_folder_phys = []
        list_dirpath_phys = []
        list_files_phys = []
        for (dirpath, dirnames, filenames) in os.walk(upload_path):
            list_dirpath_phys.extend(dirpath)
            list_folder_phys.extend(dirnames)
            list_files_phys.extend(filenames)
            break
        
        nb_files_phys = len(list_files_phys)
        
        
        #sort locations again
        sorted_keys = [key for key in sorted(upload_structure.keys())]
        
        #assume: We want to check only for the lowest container or dataset:
        val_scope = upload_structure[sorted_keys[-1]]['did'].split(":")[0]
        val_dname = upload_structure[sorted_keys[-1]]['did'].split(":")[1]
        
        #list all attached files from the rucio cataloge:
        list_files_rucio = self.rc_api.ListContent(val_scope, val_dname)
        rucio_files = []
        for i_file in list_files_rucio:
            rucio_files.append(i_file['name'])
        nb_files_rucio = len(rucio_files)
        
        #print("-----------------------")
        #print("Files on disk:", nb_files_phys)
        #print("Files in rucio:", nb_files_rucio)
        #print("-----------------------")
        
        diff_rucio = list(set(rucio_files) - set(list_files_phys))
        diff_disk  = list(set(list_files_phys) - set(rucio_files))
        
        if checksum_test == True:
            print("Implement a checksum test")
        
        if nb_files_phys == nb_files_rucio:
            return True
        else:
            return False
        
        
    
    def Upload(self, upload_structure=None, upload_path=None,
               rse=None, rse_lifetime=None,
               rse_rules=None):
        self.init()
        #inputs: upload_structure - a dictionary
        # - The key words are not import. It is important that 
        #   you can sort them: A1, A8, A6 -> A1, A6, A8
        # - The value field must include:
        #   - did: The data identifier for the the structure
        #   - type: Describes what kind of upload structure
        #           it is: container, dataset,...
        # - Data structures are created along the way
        #   of THE SORTED KEYs
        # - The LAST data strucutre receives always the upload
        #   Content:
        #print(upload_structure)
        
        #sort keys for rucio structure creation:
        sorted_keys = [key for key in sorted(upload_structure.keys())]
        
        val_before = None
        for i_nb, i_key in enumerate(sorted_keys):
            val_did = upload_structure[i_key]['did']
            val_type= upload_structure[i_key]['type']
            val_scope = val_did.split(":")[0]
            val_dname = val_did.split(":")[1]
            
            #Create scope:
            self.rc_api.CreateScope(account=self.rucio_account,
                                    scope=val_scope)
            

            if val_type=="rucio_container":
                self.rc_api.CreateContainer(val_scope, val_dname)
            elif val_type=="rucio_dataset":
                self.rc_api.CreateDataset(val_scope, val_dname)
            
            
            attach = {}
            attach['scope'] = val_scope
            attach['name'] = val_dname
            if i_nb > 0 and val_before!=None:
                print("Attach did {did} to top-level {did_top}".format(did=val_did,
                                                                       did_top=val_before))
                self.rc_api.AttachDids(val_before.split(":")[0], val_before.split(":")[1], 
                                       [attach], rse=rse)
            
            #Create a rule for the last element:
            if i_nb+1==len(sorted_keys):
                self.rc_api.AddRule([attach], 1, rse, lifetime=rse_lifetime)
            
            #Set previous scope:dname for attachments
            val_before = val_did
        
        #Start to upload to the last element
        upload_scope = upload_structure[sorted_keys[-1]]['did'].split(":")[0]
        upload_dname = upload_structure[sorted_keys[-1]]['did'].split(":")[1]
        
        upload_dict= {}
        upload_dict['rse']= rse
        upload_dict['scope']= upload_scope
        upload_dict['did']= upload_dname
        upload_dict['upload_path']= upload_path
        upload_dict['lifetime'] = rse_lifetime
        print("Upload:")
        print(upload_dict)
        self.rc_cli.CliUpload(method="upload-folder-with-did", upload_dict=upload_dict)
        
        print("")
        
        
        print("-")
        print(upload_path)
        
        #self.rc = RucioConfig.__init__(self)
        #self.rcdataformat = ConfigRucioDataFormat.__init__(self)