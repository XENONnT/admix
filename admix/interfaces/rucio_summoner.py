import sys
import tempfile
import subprocess
import datetime
import os
import json
from admix.helper.decorator import ClassCollector, NameCollector
from admix.interfaces.rucio_api import RucioAPI
from admix.interfaces.rucio_cli import RucioCLI

class RucioSummoner():
    def __init__(self, rucio_backend="API"):
        self.rucio_backend=rucio_backend
        self._rucio = None
        if self.rucio_backend=="API":
            self._rucio = ClassCollector["RucioAPI"]
        elif self.rucio_backend=="CLI":
            self._rucio = ClassCollector["RucioCLI"]
        else:
            print(f"You chose {self.rucio_backend} as Rucio backend which does not exists")
            print("Fix this!")
            exit()

    #Here comes the frontend configuration part:
    def SetRucioAccount(self, rucio_account):
        self.rucio_account = rucio_account
        self._rucio.SetRucioAccount(rucio_account)

    def SetConfigPath(self, config_path):
        self._rucio.SetConfigPath(config_path)

    def SetProxyTicket(self, proxy_path):
        self._rucio.SetProxyTicket(proxy_path)

    def SetHost(self, hostname):
        self._rucio.SetHost(hostname)

    def ConfigHost(self):
        self._rucio.ConfigHost()

    def Whoami(self):
        """Rucio Summoner:Whoami
           Results a dictionary to identify the current
           Rucio user and credentials.
        """
        return self._rucio.Whoami()

    def _VerifyStructure(self, upload_structure=None, level=-1):
        """The Rucio summoner is able to deal with
        two kinds of valid input arguments. To avoid
        a break in the command chain we verify the
        structure here first and prepare further steps.
        The two valid input arguments are:
        - A Rucio scope:name structure (DID) which is encoded
          by a string
        - A stacked container-dataset-file structure which
          is encoded in a dictionary
        """

        val_scope = None
        val_dname = None
        if isinstance(upload_structure, str):
            try:
                val_scope = upload_structure.split(":")[0]
                val_dname = upload_structure.split(":")[1]
            except IndexError as e:
                print("Function _VerifyStructure for Rucio DID input: IndexError")
                print("Message:", e)
                exit(1)
        elif isinstance(upload_structure, dict):
            #you can not sort keys if they are not in the dictionary:
            #leave like it is
            sorted_keys = [key for key in sorted(upload_structure.keys())]
            try:
                val_scope = upload_structure[sorted_keys[level]]['did'].split(":")[0]
                val_dname = upload_structure[sorted_keys[level]]['did'].split(":")[1]
            except IndexError as e:
                print("Function _VerifyStructure for Rucio template input: IndexError")
                print("Message:", e)
                exit(1)

        return (val_scope, val_dname)

    def VerifyStructure(self, upload_structure=None):

        sorted_keys = [key for key in sorted(upload_structure.keys())]
        for i_key in sorted_keys:

            #assume: We want to check only for the lowest container or dataset:
            val_scope = upload_structure[i_key]['did'].split(":")[0]
            val_dname = upload_structure[i_key]['did'].split(":")[1]
            list_files_rucio = self._rucio.ListContent(val_scope, val_dname)
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
        r_rules = self._rucio.ListDidRules(val_scope, val_dname)
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
            self._rucio.AddRule( [dids],
                                 copies=1,
                                 rse_expression=g_rse,
                                 lifetime=g_rlt)

    def UpdateRules(self, upload_structure=None, rse_rules=None):

        #sort locations again
        sorted_keys = [key for key in sorted(upload_structure.keys())]

        #assume: We want to check only for the lowest container or dataset:
        val_scope = upload_structure[sorted_keys[-1]]['did'].split(":")[0]
        val_dname = upload_structure[sorted_keys[-1]]['did'].split(":")[1]

        #Get Rule IDs:
        r_rules = self._rucio.ListDidRules(val_scope, val_dname)
        r_rse_ids = {}
        for i_rule in r_rules:
            r_rse = i_rule['rse_expression']
            r_rse_ids[r_rse] = i_rule['id']

        for i_rule in rse_rules:
            g_ptr = i_rule.split(":")[0]
            g_rse = i_rule.split(":")[1]
            g_rlt = i_rule.split(":")[2]
            if g_rlt == "None":
                g_rlt = None
            else:
                g_rlt = int(g_rlt)
            if g_rse not in list(r_rse_ids.keys()):
                continue

            print("Create", g_ptr, g_rse, g_rlt, r_rse_ids[g_rse])
            options = {}
            options['lifetime'] = g_rlt
            self._rucio.UpdateRule(r_rse_ids[g_rse], options )


    def _rule_status_dictionary(self):
        """This dictionary defines the full set of rule information
        what is returned from Rucio and dedicated to further usage.
        Add information carefully if you need to. Removing anything from
        this dictionary breaks aDMIX."""
        rule = {}
        rule['rse'] = None
        rule['exists'] = False
        rule['state'] = "Unkown"
        rule['cnt_ok'] = 0
        rule['cnt_repl'] = 0
        rule['cnt_stuck'] = 0
        rule['id'] = None
        rule['expires'] = None
        return rule

    def ListDidRules(self, upload_structure=None):
        """List existing rules for a Rucio DID or Rucio template.

        :param upload_structure: Allows a string which follows the Rucio DID structure of "scope:name" or a template
                                 which is defined by the template class and the Rucio template defintion.
        :return: A list of Rucio transfer rules with additional rule information. Each list element stands for a
                 Rucio Storage Element (RSE).
        """

        #analyse the function input regarding its allowed definitions:
        val_scope, val_dname = self._VerifyStructure(upload_structure)

        r_rules = self._rucio.ListDidRules(val_scope, val_dname)

        return list(r_rules)

    def GetRule(self, upload_structure=None, rse=None):
        """This function checks if for a given upload structure or Rucio DID a requested
        upload destination rule exists in Rucio already and returns a standarized
        dictionary
        """

        #analyse the function input regarding its allowed definitions:
        val_scope, val_dname = self._VerifyStructure(upload_structure)

        r_rules = self._rucio.ListDidRules(val_scope, val_dname)
        if len(r_rules) == 0:
            return None

        rule = self._rule_status_dictionary()
        for i_rule in r_rules:

            if rse!= None and i_rule['rse_expression'] != rse:
                continue
            rule['rse'] = i_rule['rse_expression']
            rule['exists'] = True
            rule['state']  = i_rule['state']
            rule['cnt_ok'] = i_rule['locks_ok_cnt']
            rule['cnt_repl'] = i_rule['locks_replicating_cnt']
            rule['cnt_stuck'] = i_rule['locks_stuck_cnt']
            rule['id'] = i_rule['id']
            print("_", i_rule['expires_at'], type(i_rule['expires_at']))
            if i_rule['expires_at'] == None:
                rule['expires'] = None
            else:
                rule['expires'] = i_rule['expires_at'].strftime("%Y-%m-%d-%H:%M:%S")

        return rule

    def CheckRule(self, upload_structure=None, rse=None):

        rule = self.GetRule(upload_structure, rse)
        #This function checks if for a given upload structure and a requested
        #upload destination already a rule exists in Rucio

        #HINT: GetRule returns None if no rule found now!

        r_status = None
        if rule['exists'] == False:
            r_status = "NoRule"
        elif rule['exists'] == True and rule['state'] == 'OK' and rule['cnt_ok'] > 0 and rule['cnt_repl'] == 0 and rule['cnt_stuck'] == 0:
            r_status = "OK"
        elif rule['exists'] == True and rule['state'] == 'REPLICATING':
            r_status = "REPLICATING"
        elif rule['exists'] == True and rule['state'] == 'STUCK':
            r_status = "STUCK"

        return r_status


    def VerifyLocations(self, upload_structure=None, upload_path=None, checksum_test=False):


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
        list_files_rucio = self._rucio.ListContent(val_scope, val_dname)
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

    def DownloadDids(self, dids, download_path=None, rse=None,
                     no_subdir=False, transfer_timeout=None,
                     num_threads=2, trace_custom_fields={}):

        #download a list of dids given its scope, name, path, rse:
        dw = []
        for i_did in dids:

            dw_dict = {}
            dw_dict['did'] = i_did
            dw_dict['rse'] = rse
            dw_dict['base_dir'] = download_path
            dw_dict['no_subdir'] = no_subdir
            dw_dict['transfer_timeout'] = transfer_timeout
            dw.append(dw_dict)

        dw_status = self._rucio.DownloadDids(items=dw, num_threads=num_threads, trace_custom_fields=trace_custom_fields)
        return dw_status

    def DownloadChunks(self, did, chunks, download_path=None, rse=None,
                     no_subdir=False, transfer_timeout=None,
                     num_threads=2, trace_custom_fields={}):

        #Download a single File from a given dataset or container
        #Given the prior information of the DID
        did_scope = did.split(":")[0]
        did_dname = did.split(":")[1]

        dw = []
        for i_chunk in chunks:
            dw_dict = {}
            dw_dict['did'] = "{0}:{1}-{2}".format(did_scope, did_dname, i_chunk)
            dw_dict['rse'] = rse
            dw_dict['base_dir'] = download_path
            dw_dict['no_subdir'] = no_subdir
            dw_dict['transfer_timeout'] = transfer_timeout
            dw.append(dw_dict)

        dw_status = self._rucio.DownloadDids(dw, num_threads, trace_custom_fields)
        return dw_status

    def Download(self, download_structure=None, download_path=None,
                 rse=None, level=None):

        # sort keys for rucio structure creation:
        sorted_keys = [key for key in sorted(download_structure.keys())]

        val_before = None

        for i_nb, i_key in enumerate(sorted_keys):
            val_did = download_structure[i_key]['did']
            val_type = download_structure[i_key]['type']
            val_scope = val_did.split(":")[0]
            val_dname = val_did.split(":")[1]
            print(i_nb, i_key, val_did)


    def Upload(self, upload_structure=None, upload_path=None,
               rse=None, rse_lifetime=None):

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

        upload_dict = {}
        upload_dict['path'] = upload_path+"/"
        upload_dict['rse']= rse
        upload_dict['lifetime'] = rse_lifetime
        upload_dict['did_scope'] = None

        for i_nb, i_key in enumerate(sorted_keys):
            val_did = upload_structure[i_key]['did']
            val_type= upload_structure[i_key]['type']
            val_scope = val_did.split(":")[0]
            val_dname = val_did.split(":")[1]

            #Create scope:
            self._rucio.CreateScope(account=self.rucio_account,
                                    scope=val_scope)


            if val_type=="rucio_container":
                self._rucio.CreateContainer(val_scope, val_dname)
            elif val_type=="rucio_dataset":
                self._rucio.CreateDataset(val_scope, val_dname)
                upload_dict['dataset_scope'] = val_scope
                upload_dict['dataset_name']  = val_dname
                upload_dict['did_scope'] = val_scope

            attach = {}
            attach['scope'] = val_scope
            attach['name'] = val_dname
            if i_nb > 0 and val_before!=None:
                print("Attach did {did} to top-level {did_top}".format(did=val_did,
                                                                       did_top=val_before))
                self._rucio.AttachDids(val_before.split(":")[0], val_before.split(":")[1],
                                       [attach], rse=rse)

            #Create a rule for the last element:
            if i_nb+1==len(sorted_keys):
                self._rucio.AddRule([attach], 1, rse, lifetime=rse_lifetime)

            #Set previous scope:dname for attachments
            val_before = val_did

        #Start to upload to the last element
        upload_scope = upload_structure[sorted_keys[-1]]['did'].split(":")[0]
        upload_dname = upload_structure[sorted_keys[-1]]['did'].split(":")[1]



        print("Upload:")
        print("-----------")
        print(upload_dict)
        print("-----------")

        #rc_status, rc_status_msg = self._rucio.Upload(method="upload-folder-with-did-by-file", upload_dict=upload_dict)
        rc_status, rc_status_msg = self._rucio.Upload(method="upload-folder-with-did", upload_dict=[upload_dict])
        #return rc_status, rc_status_msg



        #self.rc = RucioConfig.__init__(self)
        #self.rcdataformat = ConfigRucioDataFormat.__init__(self)
