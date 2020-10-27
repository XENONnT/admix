"""
.. module:: rucio_summoner
   :platform: Unix
   :synopsis: Run Rucio commando out of the box with complicated container/dataset structures

.. moduleauthor:: Boris Bauermeister <Boris.Bauermeister@gmail.com>

"""

import sys
import tempfile
import subprocess
import datetime
import os
import json
from admix.interfaces.rucio_api import RucioAPI
from admix.interfaces.rucio_cli import RucioCLI
from admix.helper.decorator import NameCollector, ClassCollector

import hashlib

class RucioSummoner():
    def __init__(self, rucio_backend="API"):
        self.rucio_backend=rucio_backend
        self.rucio_account = os.environ.get("RUCIO_ACCOUNT")
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

        :return A dictionary with Rucio whoami information
        """

        return self._rucio.Whoami()

    def Alive(self):
        """Function: Alive
        Simple print statement to test Rucio setup
        """
        whoami = self._rucio.Whoami()

        print("Rucio ")
        print("Rucio Whoami()")
        for ikey, ival in whoami.items():
            print(ikey, "\t \t", ival)
        print()
        print("Rucio alive")

    def _md5_hash(self, string):
        """Function: _md5_hash(...)

        Calculate a md5 hash from a string

        :param string: A string
        :return result: A md5 checksum of the input string
        """
        return hashlib.md5(string.encode('utf-8')).hexdigest()

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

        :param: upload_structure: A string (Rucio DID form of "scope:name") or a template dictionary
        :param: level: If a template dictionary is used, the level refers to the depth of the sorted dictionary at
                which the 'did' is chosen from.
        :return: (val_scope, val_dname): The extracted Rucio DID to which certain operations are applied.
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
                #exit(1)
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
                #exit(1)

        return (val_scope, val_dname)

    def _IsTemplate(self, upload_structure):
        """Function: _IsTemplate()

        :param upload_structure:  A string (Rucio DID form of "scope:name") or a template dictionary
        :return is_template: Returns True if the input is a template_dictionary, otherwise false
        """

        is_template = False

        val_scope = None
        val_dname = None

        if isinstance(upload_structure, dict):
            #you can not sort keys if they are not in the dictionary:
            #leave like it is
            sorted_keys = [key for key in sorted(upload_structure.keys())]

            level_checks = []
            for i_level in sorted_keys:
                i_level = upload_structure[i_level]
                level_check = False
                if isinstance(i_level, dict) and \
                    'did' in list(i_level.keys()) and \
                    'type' in list(i_level.keys()) and \
                    'tag_words' in list(i_level.keys()):
                    level_check = True
                level_checks.append(level_check)

            #if the list of level_checks contains one single False element
            #the input template dictionary seems to be wrong.
            if False not in level_checks:
                is_template = True

        return is_template

#TODO REMOVE LATER WHEN YOU ARE SAFE THAT THIS FUNCTION IS NOT USED AT ALL
#    def VerifyStructure(self, upload_structure=None):
#
#        sorted_keys = [key for key in sorted(upload_structure.keys())]
#        for i_key in sorted_keys:
#            #assume: We want to check only for the lowest container or dataset:
#            val_scope = upload_structure[i_key]['did'].split(":")[0]
#            val_dname = upload_structure[i_key]['did'].split(":")[1]
#            list_files_rucio = self._rucio.ListContent(val_scope, val_dname)
#            for i_rucio in list_files_rucio:
#                print(i_rucio['type'])
#        print(upload_structure)

    def AddRule(self, did, rse, lifetime=None, protocol='rucio-catalogue', priority=3):
        """Add rules for a Rucio DID or dictionary template.

        :param: did: Rucio DID form of "scope:name"
        :param: rse: An existing Rucio storage element (RSE)
        :param: lifetime: Choose a lifetime of the transfer rule in seconds or None
        :param: protocol: Should always be 'rucio-catalogue'?
        :return:
        """

        # analyse the function input regarding its allowed definitions:
        val_scope, val_dname = self._VerifyStructure(did)

        # Get current rules for this did
        rules = self._rucio.ListDidRules(val_scope, val_dname)
        current_rses = [r['rse_expression'] for r in rules]

        # if a rule already exists, exit
        if rse in current_rses:
            print("There already exists a rule for DID %s at RSE %s" % (did, rse))
            return 1

        # add rule
        did_dict= {}
        did_dict['scope'] = val_scope
        did_dict['name']  = val_dname

        self._rucio.AddRule( [did_dict],
                             copies=1,
                             rse_expression=rse,
                             lifetime=lifetime,
                             priority=priority)
        return 0


    def AddConditionalRule(self, did, from_rse, to_rse, lifetime=None, protocol='rucio-catalogue', priority=3):
        """Add rules for a Rucio DID or dictionary template.

        :param: did: Rucio DID form of "scope:name"
        :param: rse: An existing Rucio storage element (RSE)
        :param: lifetime: Choose a lifetime of the transfer rule in seconds or None
        :param: protocol: Should always be 'rucio-catalogue'?
        :return:
        """

        # analyse the function input regarding its allowed definitions:
        val_scope, val_dname = self._VerifyStructure(did)

        # Get current rules for this did
        rules = self._rucio.ListDidRules(val_scope, val_dname)
        current_rses = [r['rse_expression'] for r in rules]

        # if a rule already exists, exit
        if to_rse in current_rses:
            print("There already exists a rule for DID %s at RSE %s" % (did, to_rse))
            return 1

        # add rule
        did_dict= {}
        did_dict['scope'] = val_scope
        did_dict['name']  = val_dname

        self._rucio.AddRule( [did_dict],
                             copies=1,
                             rse_expression=to_rse,
                             source_replica_expression=from_rse,
                             lifetime=lifetime,
                             priority=priority)
        return 0


    def UpdateRules(self, upload_structure=None, rse_rules=None, level=-1):
        """Update existing rules for a Rucio DID or dictionary template.

        :param: upload_structure: A string (Rucio DID form of "scope:name") or a template dictionary
        :param: rse_rules: A list of strings which follow a certain template of ["{protocol}:{rse}:{lifetime}",...]
                           With:
                           protocol: rucio-catalogue
                           rse: An existing Rucio storage element (RSE)
                           lifetime: Choose a lifetime of the transfer rule in seconds or None
        :param: level: If a template dictionary is used, the level refers to the depth of the sorted dictionary at
                       which the 'did' is chosen from.
        :return result: A dictionary with Rucio Storage Elements (RSE) as keys. The value is another dictionary
                        with keys 'result' (0 on success, 1 on failure) and lifetime ( an integer > 0)
        """

        #analyse the function input regarding its allowed definitions:
        val_scope, val_dname = self._VerifyStructure(upload_structure, level)

        #Get Rule IDs:
        r_rules = self._rucio.ListDidRules(val_scope, val_dname)
        r_rse_ids = {}
        for i_rule in r_rules:
            r_rse = i_rule['rse_expression']
            r_rse_ids[r_rse] = i_rule['id']

        result = {}
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

            options = {}
            options['lifetime'] = g_rlt
            r = self._rucio.UpdateRule(r_rse_ids[g_rse], options )

            result[g_rse] = {}
            result[g_rse]['result'] = r
            result[g_rse]['lifetime'] = g_rlt

        return result

    def DeleteRule(self, rule_id):
        self._rucio.DeleteRule(rule_id)

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

    def ListDidRules(self, upload_structure=None, level=-1):
        """List existing rules for a Rucio DID or dictionary template.

        :param: upload_structure: A string (Rucio DID form of "scope:name") or a template dictionary
        :param: level: If a template dictionary is used, the level refers to the depth of the sorted dictionary at
                which the 'did' is chosen from.
        :return: A list of Rucio transfer rules with additional rule information. Each list element stands for a
                 Rucio Storage Element (RSE). If no rule exists it returns an empty list

        """

        #analyse the function input regarding its allowed definitions:
        val_scope, val_dname = self._VerifyStructure(upload_structure, level)

        r_rules = self._rucio.ListDidRules(val_scope, val_dname)

        return r_rules

    def ListFileReplicas(self, upload_structure=None, rse=None, level=-1, localpath=False):
        """Function: ListFileReplicas(...)

        List all your file replicas which are attached to a dataset or container.

        Hint: List of RSE wide file replicas (local path) was not available in Rucio 1.19.

        :param upload_structure: A string (Rucio DID form of "scope:name") or a template dictionary
        :param rse: A valid Rucio Storage Element (RSE) of the current Rucio setting.
        :param level: If a template dictionary is used, the level refers to the depth of the sorted dictionary at
                      which the 'did' is chosen from.
        :return result: Dictionary which with key->value ordering follows:\n
                         - key: filename of the attached file\n
                         - value: The local file location for the selected RSE\n
                        Otherwise: {}

        """

        # Prepare a return dictionary:
        result = {}

        #Begin to list all files which are in the upload_structure:
        file_list = [ i_file['name'] for i_file in self.ListFiles(upload_structure, level)]

        #analyse the function input regarding its allowed definitions:
        val_scope, val_dname = self._VerifyStructure(upload_structure, level)

        #Extract all valid RSEs in the current Rucio setup and return [] if rse not valid
        list_rse = [ i_rse['rse'] for i_rse in list(self._rucio.ListRSEs())]
        if rse not in list_rse:
            return result

        # get RSE overview if RSE exists
        rse_overview = self._rucio.GetRSE(rse)

        # check if the given RSE is tape (true for yes)
        _istape = False
        if rse_overview.get('rse_type') == "DISK":
            _istape=False
        else:
            _istape=True

        rse_hostname = rse_overview['protocols'][0]['hostname']
        rse_prefix = rse_overview['protocols'][0]['prefix']
        rse_port = rse_overview['protocols'][0]['port']
        rse_scheme = rse_overview['protocols'][0]['scheme']

        # Prepare basic scheme for RSE locations:
        lfn = None
        lfn_disk = "{protocol}://{hostname}:{port}{prefix}/{scope}/{h1}/{h2}/{fname}"
        lfn_local_disk = "{prefix}/{scope}/{h1}/{h2}/{fname}"
        lfn_tape = "{protocol}://{hostname}:{port}{prefix}/{scope}/{fname}"


        #Start to fill the dictionary according to istape==True or False
        if _istape==False:
            # assume a disk storage (deterministic paths):
            for i_filename in file_list:
                # Calculate the deterministic hash sums:
                rucio_did = "{scope}:{name}".format(scope=val_scope,
                                                    name=i_filename)
                t1 = self._md5_hash(rucio_did)[0:2]
                t2 = self._md5_hash(rucio_did)[2:4]

                if localpath:
                    lfn = lfn_local_disk.format(prefix=rse_prefix,
                                          scope=val_scope,
                                          h1=t1,
                                          h2=t2,
                                          fname=i_filename)
                    result[i_filename] = lfn
                else:
                    lfn = lfn_disk.format(protocol=rse_scheme,
                                          hostname=rse_hostname,
                                          port=rse_port,
                                          prefix=rse_prefix,
                                          scope=val_scope,
                                          h1=t1,
                                          h2=t2,
                                          fname=i_filename)
                    result[i_filename] = lfn
                    

        else:
            #assume tape storage (non-deterministic paths)
            for i_filename in file_list:
                lfn = lfn_tape.format(protocol=rse_scheme,
                                      hostname=rse_hostname,
                                      port=rse_port,
                                      prefix=rse_prefix,
                                      scope=val_scope,
                                      fname=i_filename)
                result[i_filename] = lfn

        return result

    def ListFiles(self, upload_structure=None, long=None, level=-1):
        """List existing files for a Rucio DID or dictionary template.

        :param: upload_structure: A string (Rucio DID form of "scope:name") or a template dictionary
        :param: level: If a template dictionary is used, the level refers to the depth of the sorted dictionary at
                which the 'did' is chosen from.
        :param long: Define another output (Check the Rucio tutorials for it)
        :return result: A list of files, otherwise []
        """

        #analyse the function input regarding its allowed definitions:
        val_scope, val_dname = self._VerifyStructure(upload_structure, level)

        result = []
        result = self._rucio.ListFiles(val_scope, val_dname, long=long)

        return result

    def ListDids(self,scope, filters, type='collection', long=False, recursive=False):
        """
        List all data identifiers in a scope which match a given pattern. Check Rucio github page for details

        :param scope: The valid string which follows the Rucio scope name.
        :param filters: A dictionary of key/value pairs like {'name': 'file_name','rse-expression': 'tier0'}.
        :param type: The type of the did: 'all'(container, dataset or file)|'collection'(dataset or container)|'dataset'|'container'|'file'
        :param long: Long format option to display more information for each DID.
        :param result: Recursively list DIDs content.
        """

        result = []
        result = self._rucio.ListDids(scope=scope,
                                      filters=filters,
                                      type=type,
                                      long=long,
                                      recursive=recursive)

        return result


    def GetRule(self, upload_structure=None, rse=None, level=-1):
        """This function checks if for a given upload structure or Rucio DID a requested
        upload destination rule exists in Rucio already and returns a standardized
        dictionary.

        :param: upload_structure: A string (Rucio DID form of "scope:name") or a template dictionary
        :param: rse: A valid Rucio Storage Element (RSE)
        :param: level: If a template dictionary is used, the level refers to the depth of the sorted dictionary at
                which the 'did' is chosen from.
        :return rule: A dictionary of pre-definied rule information.
        """

        #analyse the function input regarding its allowed definitions:
        val_scope, val_dname = self._VerifyStructure(upload_structure, level)

        r_rules = self._rucio.ListDidRules(val_scope, val_dname)
        if len(r_rules) == 0:
            return self._rule_status_dictionary()

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
            #print("_", i_rule['expires_at'], type(i_rule['expires_at']))
            if i_rule['expires_at'] == None:
                rule['expires'] = None
            else:
                rule['expires'] = i_rule['expires_at'].strftime("%Y-%m-%d-%H:%M:%S")

        return rule

    def CheckRule(self, upload_structure=None, rse=None, level=-1):
        """Check the status message for a Rucio DID or dictionary template rule.
        This is a shortcut in combination with the memberfunction GetRule()

        :param: upload_structure: A string (Rucio DID form of "scope:name") or a template dictionary. The member
                function GetRule(...) evaluates the upload_structure variable.
        :param: rse: Specify a valid Rucio Storage Element (RSE)
        :param: level: If a template dictionary is used, the level refers to the depth of the sorted dictionary at
                which the 'did' is chosen from.
        :return: r_status: Rucio rule status: OK, STUCK or REPLICATING. If rule does not exists returns NoRule
        """

        if rse == None:
            return "NoRule"

        rule = self.GetRule(upload_structure, rse, level)
        #This function checks if for a given upload structure and a requested
        #upload destination already a rule exists in Rucio

        #HINT: GetRule returns None if no rule found now!
        #print(rule)

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


    def VerifyLocations(self, upload_structure=None, upload_path=None, checksum_test=False, level=-1):
        """This function checks if for a given upload structure or Rucio DID a requested
        upload destination rule exists in Rucio already and returns a standardized
        dictionary.

        :param upload_structure: A string (Rucio DID form of "scope:name") or a template dictionary
        :param upload_path: A path which holds the files for the Rucio upload
        :param checksum_test: Enable extended checksum test with True.
        :param level: If a template dictionary is used, the level refers to the depth of the sorted dictionary at
                which the 'did' is chosen from.
        :return (check_success, diff_rucio, diff_disk): check_success True if the same files in Rucio such on disk
                                                        otherwise False.
                                                        diff_rucio returns a list of files which are in Rucio but not
                                                        on disk.
                                                        diff_disk returns a list of files which are on disk but not
                                                        attached to the Rucio DID.
        """

        #analyse the function input regarding its allowed definitions:
        val_scope, val_dname = self._VerifyStructure(upload_structure, level)

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


        #list all attached files from the rucio cataloge:
        list_files_rucio = self._rucio.ListContent(val_scope, val_dname)

        rucio_files = []
        for i_file in list_files_rucio:
            rucio_files.append(i_file['name'])
        nb_files_rucio = len(rucio_files)

        #calculate diff of the filename lists
        #Files which are Rucio but not on disk:
        diff_rucio = list(set(rucio_files) - set(list_files_phys))
        #Files which are on disk but not in Rucio
        diff_disk  = list(set(list_files_phys) - set(rucio_files))

        #ToDo: Need a checksum test
        if checksum_test == True:
            print("Implement a checksum test")
            pass

        if nb_files_phys == nb_files_rucio:
            return (True, diff_rucio, diff_disk)
        else:
            return (False, diff_rucio, diff_disk)

    #Rucio download section
    def DownloadDids(self, dids=None, download_path=".", rse=None,
                     no_subdir=False, transfer_timeout=None,
                     num_threads=3, trace_custom_fields={}):
        """Function: DownloadDids(...)

        This functions offers to download a list if Rucio DIDs which are given by a list.

        :param dids: A string or a list of strings which follows the Rucio DID nameing convention
        :param download_path: Path to store the downloaded data
        :param rse: Specify the RSE from where the data are going to be downloaded
        :param no_subdir: True if no sub directory is going to be created.
        :param transfer_timeout: Wait for so many seconds and try to continue with the download (optional)
        :param num_threads: Standard two (2) threads are used for downloading on a CPU (optional)
        :param trace_custom_fields: Customize download, see Rucio tutorials optional)
        :return result: A list of Rucio download information as a list of dictionaries. If it fails 1
        """

        #Return failure if no dids information is given:
        if dids == None:
            return 1

        #To enforce the correct input on dids:
        if isinstance(dids, str):
            dids = [dids]

        #Create the list of dictionaries for download
        dw = []
        for i_did in dids:

            dw_dict = {}
            dw_dict['did'] = i_did
            dw_dict['rse'] = rse
            dw_dict['base_dir'] = download_path
            dw_dict['no_subdir'] = no_subdir
            dw_dict['transfer_timeout'] = transfer_timeout
            dw.append(dw_dict)

        #Download from Rucio
        result = self._rucio.DownloadDids(items=dw,
                                          num_threads=num_threads,
                                          trace_custom_fields=trace_custom_fields)

        return result

    def DownloadChunks(self, download_structure=None, chunks=None, download_path=".",
                       rse=None, no_subdir=False, transfer_timeout=None,
                       num_threads=2, trace_custom_fields={}, level=-1):
        """Function: DownloadChunks(...)

        This function offers to download specific chunks from Rucio a specific DiD.
        Warning: This function is heavily made for XENON internal structures. Please use with care.

        :param download_structure: A valid Rucio DID (string) or a template dictionary of an existing DID
        :param download_path: Path to store the downloaded data
        :param rse: Specify the RSE from where the data are going to be downloaded
        :param chunks: A list (strings) chunk numbers (Format: XXXXXX (six digits), or metadata.json:
                       Example: list=['000000', '000001', 'metadata.json']
        :param no_subdir: True if no sub directory is going to be created.
        :param transfer_timeout: Wait for so many seconds and try to continue with the download (optional)
        :param num_threads: Standard two (2) threads are used for downloading on a CPU (optional)
        :param trace_custom_fields: Customize download, see Rucio tutorials optional)
        :param level: Specify the download DID from a template dictionary if it is hand over (optional, the last level
                      is chosen)
        :return result: A list of Rucio download information as a list of dictionaries. If it fails 1
        """

        #Begin with analysing the download structure (similar to upload_structure)
        val_scope, val_dname = self._VerifyStructure(download_structure, level)

        dw = []
        for i_chunk in chunks:
            dw_dict = {}
            dw_dict['did'] = "{0}:{1}-{2}".format(val_scope, val_dname, i_chunk)
            dw_dict['rse'] = rse
            dw_dict['base_dir'] = download_path
            dw_dict['no_subdir'] = no_subdir
            dw_dict['transfer_timeout'] = transfer_timeout
            dw.append(dw_dict)

        result = self._rucio.DownloadDids(dw, num_threads, trace_custom_fields)

        return result

    def Download(self, download_structure=None, download_path=".",
                 rse=None, no_subdir=False, transfer_timeout=None,
                 num_threads=2, trace_custom_fields={}, level=-1):
        """Function: Download(...)

        This function offers to download from Rucio a specific DiD.

        :param download_structure: A valid Rucio DID (string) or a template dictionary of an existing DID
        :param download_path: Path to store the downloaded data
        :param rse: Specify the RSE from where the data are going to be downloaded
        :param no_subdir: True if no sub directory is going to be created.
        :param transfer_timeout: Wait for so many seconds and try to continue with the download (optinal)
        :param num_threads: Standard two (2) threads are used for downloading on a CPU (optinal)
        :param trace_custom_fields: Customize download, see Rucio tutorials (optinal)
        :param level: Specify the download DID from a template dictionary if it is hand over (optional, the last level
                      is chosen)
        :return result: A list of Rucio download information as a list of dictionaries. If it fails 1
        """

        #Begin with analysing the download structure (similar to upload_structure)
        val_scope, val_dname = self._VerifyStructure(download_structure, level)

        #Prepare a download dictionary:
        dw_dict = {}
        dw_dict['did'] = "{0}:{1}".format(val_scope, val_dname)
        dw_dict['rse'] = rse
        dw_dict['base_dir'] = download_path
        dw_dict['no_subdir'] = no_subdir
        dw_dict['transfer_timeout'] = transfer_timeout

        result = self._rucio.DownloadDids(dw_dict, num_threads, trace_custom_fields)

        return result


    #----Rucio upload section

    def UploadToScope(self, scope=None, upload_path=None,
                      rse=None, rse_lifetime=None):
        """Function: UploadToScope()

        Upload a folder to a Rucio scope

        :param scope: A string which follows the rules of Rucio string
        :param upload_path: A valid (string) to a folder which holds a file (or files) for upload
        :param rse: A valid Rucio Storage Element (RSE)
        :param rse_lifetime: A valid (int) which defines the lifetime of the transfer rule after upload.
        :return result: (upload_status, rse_rule) means:\n
                * (0, {'result': 0, 'lifetime': rse_lifetime}) for success and applied lifetime to the rule\n
                * (0, 1) for success and no rse_lifetime to the rule\n
                * (1, 1) for upload failure and rse_lifetime is not given\n
                * (1, {'result':1, 'lifetime': rse_lifetime}) for upload failure and rse_lifetime is skipped automatically
        """

        result = 1
        result_rule = 1

        if isinstance(scope, str) == False:
            print("Function UploadToScope() needs an Rucio (str) scope as input")
            exit(1)
        if rse==None:
            print("No Rucio Storage Element (rse) given.")
            exit(1)
        if upload_path ==None:
            print("No path/file given for upload")
            exit(1)

        #get all files from the physical location:
        list_files_phys = []
        for (dirpath, dirnames, filenames) in os.walk(upload_path):
            list_files_phys.extend(filenames)
            break

        #Create Scope:
        result = self._rucio.CreateScope(account=self.rucio_account,
                                scope=scope)
        #result 0 - success or 1 - failure

        upload_dict = {}
        upload_dict['path'] = upload_path + "/"
        upload_dict['rse'] = rse
        upload_dict['did_scope'] = scope

        result = self._rucio.Upload(upload_dict=[upload_dict])

        if rse_lifetime != None and isinstance(rse_lifetime, int) and \
            rse_lifetime > 0:# and \
            #result==0:
            # create an UpdateRule() conform input:
            rule = [f"rucio-catalogue:{rse}:{rse_lifetime}"]

            #apply continuously:
            result_rule_count = []
            for i_file in list_files_phys:
                upload_structure=f"{scope}:{i_file}"
                result_count = self.UpdateRules(upload_structure=upload_structure, rse_rules=rule)
                #evaluate result
                result_rule_count.append(result_count.get(rse,1))
            k_count = 0
            for ik in result_rule_count:
                k_count+=ik['result']

            if k_count == 0:
                result_rule = 0

        return (result, result_rule)

    def UploadToDid(self, upload_structure=None, upload_path=None,
               rse=None, rse_lifetime=None):
        """Function UploadToDid()

        This function uploads the content of given folder into a Rucio dataset
        which is identified by given DID.

        For example a folder:

        | /path/to/example/calibration_source_1
        |                   │
        |                   ├──18_t2_01
        |                   ├──18_t2_02
        |                   └──18_t2_03

        DID (dataset): calibration_data_day1:calibration_source_1

        Results a Rucio structure:

        | calibration_data_day1:calibration_source_1        (Rucio dataset)
        |           │
        |           ├──calibration_data_day1:18_t2_01   (Rucio file attached to dataset)
        |           ├──calibration_data_day1:18_t2_02   (Rucio file attached to dataset)
        |           └──calibration_data_day1:18_t2_03   (Rucio file attached to dataset)


        :param upload_structure: A Rucio DID (type str) in form of scope:name
        :param upload_path: A path to an existing folder with data for upload
        :param rse: A valid Rucio Storage Element (RSE) for the initial upload
        :param rse_lifetime: (Optional) A lifetime in seconds
        :return result: (upload_status, rse_rule) means:\n
                  * (0, {'result': 0, 'lifetime': rse_lifetime}) for success and applied lifetime to the rule\n
                  * (0, 1) for success and no rse_lifetime to the rule\n
                  * (1, 1) for upload failure and rse_lifetime is not give\n
                  * (1, {'result':1, 'lifetime': rse_lifetime}) for upload failure and rse_lifetime is skipped automatically
        """

        result = 1
        result_rule = 1

        #Go into
        if isinstance(upload_structure, str) == False and ":" not in upload_structure:
            print("Function UploadDid() needs an Rucio (str) DID as input (scope:name)")
            return (1, 1)
        if rse==None:
            print("No Rucio Storage Element (rse) given.")
            return (1, 1)
        if upload_path ==None:
            print("No path/file given for upload")
            return (1, 1)

        upload_scope = upload_structure.split(":")[0]
        upload_dname = upload_structure.split(":")[1]

        #Create Scope:
        result = self._rucio.CreateScope(account=self.rucio_account,
                                scope=upload_scope)
        #result 0 - success or 1 - failure

        #create the upload dictionary:
        upload_dict = {}
        upload_dict['path'] = upload_path + "/"
        upload_dict['rse'] = rse
        upload_dict['lifetime'] = rse_lifetime
        upload_dict['dataset_scope'] = upload_scope
        upload_dict['dataset_name'] = upload_dname
        upload_dict['did_scope'] = upload_scope

        result = self._rucio.Upload(upload_dict=[upload_dict])

        #We are ready here but we can apply a lifetime to the auto generated rule:
        #Obs! Rule lifetime only applied if upload was successful!
        if rse_lifetime != None and isinstance(rse_lifetime, int) and \
            rse_lifetime > 0 and \
            result==0:

            #create an UpdateRule() conform input:
            rule = [f"rucio-catalogue:{rse}:{rse_lifetime}"]
            #apply
            result_rule = self.UpdateRules(upload_structure=upload_structure, rse_rules=rule)
            #evaluate result
            result_rule = result_rule.get(rse, 1)

        return (result, result_rule)


    def Upload(self, did, upload_path, rse, lifetime=None):
        """Function: Upload(...)
        The data files of the upload_path are always uploaded to the last Rucio dataset.

        :param upload_structure: A string (Rucio DID form of "scope:name") or a template dictionary
        :param upload_path: The absolute path of your dataset
        :param rse: A valid Rucio Storage Element (RSE) for the upload
        :param rse_lifetime: The lifetime of the dataset (lowest level of template dictionary) after the upload
                             Hint: dataset lifetimes below 24h (86400 sec) are automatically set to 86400 sec.
        :param level: If a template dictionary is used, the level refers to the depth of the sorted dictionary at
                which the 'did' is chosen from.
        :return result: 0 for success, 1 for failure
        """


        scope, dataset = did.split(':')


        # create scope. If scope exists already, exception will be handled silently unless verbose=True is passed
        self._rucio.CreateScope(account=self.rucio_account, scope=scope)

        # create dataset
        self._rucio.CreateDataset(scope, dataset)

        # add a rule for this dataset
        self._rucio.AddRule([dict(scope=scope, name=dataset)], 1, rse, lifetime=lifetime)


        # smallest lifetime is 1 day?
        if lifetime != None and int(lifetime) < 86400:
            lifetime = 86400

        #Prepare an upload dictionary which is used later to upload to Rucio
        upload_dict = dict(path=upload_path + "/",
                           rse=rse,
                           lifetime=lifetime,
                           did_scope=scope,
                           dataset_scope=scope,
                           dataset_name=dataset
                           )

        # finally, upload the dataset
        result = self._rucio.Upload(upload_dict=[upload_dict])
        return result
