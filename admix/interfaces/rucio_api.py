"""
.. module:: rucio_api
   :platform: Unix
   :synopsis: An extensive wrapper for the Rucio API in aDMIX

.. moduleauthor:: Boris Bauermeister <Boris.Bauermeister@gmail.com>

"""
#from __future__ import with_statement
import os
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
#from admix.interfaces.uploadclient import UploadClient
from rucio.client.downloadclient import DownloadClient
from rucio.common.exception import DataIdentifierAlreadyExists
from rucio.common.exception import AccountNotFound
from rucio.common.exception import AccessDenied
from rucio.common.exception import Duplicate
from rucio.common.exception import NoFilesUploaded
from rucio.common.exception import NotAllFilesUploaded
from rucio.common.exception import DuplicateContent
from rucio.common.exception import DuplicateRule

from admix.helper.decorator import NameCollector, ClassCollector
from admix.helper.decorator import Collector
import sys
import tempfile
import subprocess
import datetime
import os
import json

@Collector
class RucioAPI():
    """Class RucioAPI()

    This class presents an approach to collect all necessary Rucio calls
    in one class. That allows easy handling of Rucio calls based on the
    Rucio API.
    If there are ever changes in the Rucio API, here is the wrapper to fix it.
    """

    def __init__(self, enable_print=False):
        """Function: __init__()

        Constructor of RucioAPI class. Comes with a setting set up the print statements to terminal

        :param enable_print: If True then enable print to terminal
        """
        self._print_to_screen = enable_print
        self._rucio_ping = None
        self._rucio_account = os.environ.get("RUCIO_ACCOUNT")
        self.ConfigHost()

    def __del__(self):
        """Function: __del__()

        Destructor - No further description
        """
        pass

    # Here comes the backend configuration part:
    def SetRucioAccount(self, rucio_account=None):
        """Function: SetRucioAccount
        :param rucio_account: The Rucio account you would like to work with
        """

        self._rucio_account = rucio_account

    def SetConfigPath(self, config_path=None):
        """Function: SetConfigPath
        This option is only important for legacy command line support and
        is ignored in RucioAPI setup.
        :param config_path: Path to CLI configuration file
        """
        pass
    def SetProxyTicket(self, proxy_path=None):
        """Function: SetProxyTicket
        This option is only important for legacy command line support and
        is ignored in RucioAPI setup.
        :param proxy_path: Path to CLI configuration file
        """
        pass
    def SetHost(self, hostname=None):
        """Function: SetHost
        This option is only important for legacy command line support and
        is ignored in RucioAPI setup.
        :param hostname: Path to CLI configuration file
        """
        pass
    def ConfigHost(self):
        """Function: ConfigHost

        This member function setup the rucioAPI backend.
        To ensure full functionality, it needs:
        * Client()
        * UploadClient()
        * DownloadClient()

        :raise Exception if Rucio API is not ready (miss-configured)
        """
        try:
            self._rucio_client = Client()
            self._rucio_client_upload = UploadClient()
#            self._rucio_client_upload = UploadClient(tracing=False)
#            print("Tracing set to False")
            self._rucio_client_download = DownloadClient()
            self._rucio_ping = self._rucio_client.ping

        except:
            print("Can not init the Rucio API")
            print("-> Check for your Rucio installation")
            exit(1)


    # finished the backend configuration for the Rucio API

    def Whoami(self):
        """RucioAPI:Whoami
        Results a dictionary to identify the current
        Rucio user and credentials.
        """
        return self._rucio_client.whoami()

    def GetRucioPing(self):
        """Function: GetRucioPing
        :return If ConfigHost is executed without execption GetRucioPing provides a Rucio ping
        """

        return self._rucio_client.ping

    #The scope section:
    def CreateScope(self, account, scope, verbose=False):
        """Function: CreateScope()

        Create a new Rucio scope what does not yet exists yet.
        Be aware that you need Rucio permissions to do it. Check your Rucio account and settings.

        :param account: The Rucio account you are working with (need to be allowed to create scopes)
        :param scope: The scope name you like to create
        :return result:
        """

        result = 1
        try:
            self._rucio_client.add_scope(account, scope)
            result = 0
        except AccessDenied as e:
            print(e)
        except Duplicate as e:
            if verbose:
                print(e)
            else:
                pass
        return result

        #Several list commands

    def GetRSE(self, rse):
        """Function: GetRSE(...)

        Return further information about the RSE setup of a specific RSE
        :param rse: A (string) valid Rucio Storage Element (RSE) name
        :return result: A dictionary which holds information according the selected RSE
        """

        result = {}
        try:
            result = self._rucio_client.get_rse(rse)
        except:
            print("No RSE attributes received for {0}".format(rse))
        return result

    def ListRSEAttributes(self, rse):
        """Function: ListRSEAttributes(...)

        Return some attributes of a Rucio Storage Element
        Received keys are fts, fts_testing, RSE-NAME, istape

        :param rse: A valid (string) Rucio Storage Element (RSE) name
        :return result: A dictionary with RSE attributes
        """

        result = {}

        try:
            result = self._rucio_client.list_rse_attributes(rse)
        except:
            print("No RSE attributes received for {0}".format(rse))
        return result


    def ListRSEs(self):
        """Function: ListRSEs

        Returns an overview about all registered Rucio Storage elements in the current setup

        :return result: A list of dictionaries. Each dictionary holds RSE information. If not successful []
        """
        result = []
        try:
            result = list(self._rucio_client.list_rses())
        except:
            print("No RSE received from Rucio.")

        return result

    def ListContent(self, scope, name):
        """Function: ListContent()

        :param scope: A string which refers to a Rucio scope
        :param name: A string which refers to a Rucio name
        :return result: A list of dictionaries with the attached files to the DID
        """
        result = []
        try:
            return list(self._rucio_client.list_content(scope, name))
        except TypeError as e:
            print(e)

        return result

    def ListScopes(self):
        """Function: ListScopes()

        List all created scopes in the Rucio catalogue

        :return result: A list of scopes, otherwise []
        """
        result = []
        try:
            result = self._rucio_client.list_scopes()
        except:
            print("No scopes? - Check that!")
        return result

    def ListFileReplicas(self, scope, lfn):
        """Function: ListFileReplicas(...)

        List all your files which are attached to a dataset or container

        :param scope: A string which follows the rules of a Rucio scope
        :param lfn: the lfn.
        :return result: A list of file replicas, otherwise []
        """

        #todo FIX ME
        result = []
        result = self._rucio_client.list_file_replicas(scope, lfn)
        return result

        #try:
        #    result = self._rucio_client.list_file_replicas(scope, lfn)
        #except AttributeError as e:
        #    print(e)
        return result

    def ListFiles(self, scope, name, long=True):
        """Function: ListFiles(...)

        List all your files which are attached to a dataset or container

        :param scope: A string which follows the rules of a Rucio scope
        :param name: A string which follows the rules of a Rucio name
        :param long: Define another output (Check the Rucio tutorials for it)
        :return result: A list of files, otherwise []
        """
        result = []
        try:
            result = self._rucio_client.list_files(scope, name, long=None)
        except:
            print("No files are listed for {0}:{1}".format(scope, name))
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
        try:
            return list(self._rucio_client.list_dids(scope, filters, type, long, recursive))
        except TypeError as e:
            print(e)
        return result


    def ListDidRules(self, scope, name):
        """Return a class generator from Rucio which contains the
        individual rules to iterate over (or to create a list from)

        :param scope: A string which refers to the Rucio scope
        :param name: A string which refers to the Rucio name (a container, dataset or file name)

        :return: A list of Rucio transfer rules with additional rule information. Each list element stands for a
                 Rucio Storage Element (RSE). List is empty if not successful or nor rules.
        """

        result = []
        try:
            return list(self._rucio_client.list_did_rules(scope, name))
        except TypeError as e:
            print(e)

        return result

    #Attach and detach:
    def AttachDids(self, scope, name, attachment, rse=None):
        """Function: AttachDids(...)

        This function allows to attach datasets or containers to a top-level dataset or container.
        The parameters scope and name define the top-level structure (container or dataset) and the dictionary or
        the list of dictionaries contains the information about what is attached to the top-level structure.

        More information under https://github.com/rucio/rucio


        :param scope: A string which follows the rules of a Rucio scope
        :param name: A string which follows the rules of a Rucio name
        :param attachment: A dictionary or a list of dictionaries which consist of two keys: scope and name
                           example{'scope': 'example_scope1', 'name':'example_name1'}
        :param rse: The RSE name when registering replicas. (optional)
        :return result: 0 if successful, 1 for failure
        """
        result = 1

        #In case there is only an individual dictionary provided, the dictionary is transformed into a list of
        #dictionaries.
        if isinstance(attachment, dict) == True:
            attachment = [attachment]

        #self._rucio_client.attach_dids(scope, name, attachment, rse=rse)

        try:
            self._rucio_client.attach_dids(scope, name, attachment, rse=rse)
            result = 0
        except DuplicateContent as e:
            print(e)

        return result

    def DetachDids(self, scope, name, dids):
        try:
            self._rucio_client.detach_dids(scope, name, dids)
        except:
            return None

    #Container and Dataset managment:
    def CreateContainer(self, scope, name, statuses=None, meta=None, rules=None, lifetime=None):
        """Function CreateContainer(...)

        Follows the Rucio API to create a Rucio container based on scope and container name. It accept also further
        Rucio features.
        More information under https://github.com/rucio/rucio

        :param scope: A string which follows the rules of a Rucio scope
        :param name: A string which follows the rules of a Rucio container name
        :param statuses: Status (optional)
        :param meta: Put in further meta data which are going to be connected to the container. (optional)
        :param rules: Define transfer rules which apply to the container immediately. (optional)
        :param lifetime: Set a Rucio lifetime to the container if you with (optional)
        :return result: 0 if successful, 1 for failure
        """
        result = 1
        try:
            self._rucio_client.add_container(scope, name, statuses=None, meta=None, rules=None, lifetime=None)
            result = 0
        except DataIdentifierAlreadyExists as e:
            print(e)
        return result

    def CreateDataset(self, scope, name, statuses=None, meta=None,
                      rules=None, lifetime=None, files=None, rse=None, verbose=False):
        """Function CreateDataset(...)

        Follows the Rucio API to create a Rucio dataset based on scope and dataset name. It accept also further
        Rucio features.
        More information under https://github.com/rucio/rucio

        :param scope:    A string which follows the rules of a Rucio scope
        :param name:     A string which follows the rules of a Rucio dataset name
        :param statuses: Status (optional)
        :param meta:     Put in further meta data which are going to be connected to the container. (optional)
        :param rules:    Define transfer rules which apply to the container immediately. (optional)
        :param lifetime: Set a Rucio lifetime to the container if you with (optional)
        :param verbose:  Flag to print DataIdentifierAlreadyExists exceptions
        :return result:  0 if successful, 1 for failure
        """
        result = 1
        try:
            self._rucio_client.add_dataset(scope, name, statuses=None, meta=None, rules=None, lifetime=None,\
                                           files=None, rse=None)
            result = 0
        except DataIdentifierAlreadyExists as e:
            if verbose:
                print(e)
        return result

    #Rules:
    def AddRule(self, dids, copies, rse_expression, weight=None, lifetime=None, grouping='DATASET', account=None,
                locked=False, source_replica_expression=None, activity=None, notify='N', purge_replicas=False,
                ignore_availability=False, comment=None, ask_approval=False, asynchronous=False, priority=3, meta=None):
        """Function: AddRule(...)

        A function to add a Rucio transfer rule to the given Rucio data identifiers (DIDs)
        More information under https://github.com/rucio/rucio

        :param dids:                       The data identifier set.
        :param copies:                     The number of replicas.
        :param rse_expression:             Boolean string expression to give the list of RSEs.
        :param weight:                     If the weighting option of the replication rule is used, the choice of RSEs takes their weight into account.
        :param lifetime:                   The lifetime of the replication rules (in seconds).
        :param grouping:                   ALL -  All files will be replicated to the same RSE.
                                           DATASET - All files in the same dataset will be replicated to the same RSE.
                                           NONE - Files will be completely spread over all allowed RSEs without any grouping considerations at all.
        :param account:                    The account owning the rule.
        :param locked:                     If the rule is locked, it cannot be deleted.
        :param source_replica_expression:  RSE Expression for RSEs to be considered for source replicas.
        :param activity:                   Transfer Activity to be passed to FTS.
        :param notify:                     Notification setting for the rule (Y, N, C).
        :param purge_replicas:             When the rule gets deleted purge the associated replicas immediately.
        :param ignore_availability:        Option to ignore the availability of RSEs.
        :param ask_approval:               Ask for approval of this replication rule.
        :param asynchronous:               Create rule asynchronously by judge-injector.
        :param priority:                   Priority of the transfers.
        :param comment:                    Comment about the rule.
        :param meta:                       Metadata, as dictionary.

        :return result:  0 if successful, 1 for failure
        """
        result = 1

        try:
#            self._rucio_client.add_replication_rule(dids, copies, rse_expression, weight=None, lifetime=lifetime,
#                                                    grouping='DATASET', account=None, locked=False,
#                                                    source_replica_expression=None, activity=None, notify='N',
#                                                    purge_replicas=False, ignore_availability=False, comment=None,
#                                                    ask_approval=False, asynchronous=False, priority=3)
            self._rucio_client.add_replication_rule(dids, copies, rse_expression, weight=None, lifetime=lifetime,
                                                    grouping='DATASET', account=None, locked=False,
                                                    source_replica_expression=source_replica_expression, activity=None, notify='N',
                                                    purge_replicas=False, ignore_availability=False, comment=None,
                                                    ask_approval=False, asynchronous=False, priority=priority)
            result = 0
        except DuplicateRule as e:
            print(e)

        return result

    def UpdateRule(self, rule_id, options=None):
        """Function UpdateRule()

        Aims to update a particular rule according to its rule_id and further option such as lifetime
        :param rule_id: A Rucio rule id string
        :param options: A dictionary with certain options (e.g. lifetime, weight, ,...)
        :return result: 0 on success, 1 at failure
        """
        result = 1
        try:
            self._rucio_client.update_replication_rule(rule_id, options)
            result = 0
        except:
            print("Raised exception in UpdateRule")

        return result

    def GetReplicationRule(self, rule_id, estimate_ttc=False):
        """Function: GetReplicationRule(...)

        Get information on the replication rule based on the rule ID

        :param rule_id: A valid Rucio rule ID
        :return result: Information on the replication rule, otherwise 1
        """
        result = 1
        try:
            result = self._rucio_client.get_replication_rule(self, rule_id, estimate_ttc=False)
        except:
            print("No replication rule to get")
        return result

    def DeleteRule(self, rule_id):
        """Function: DeleteRule(...)
        
        Deletes a replication rule.
        :param rule_id: A rucio rule id string
        """
        self._rucio_client.delete_replication_rule(rule_id, purge_replicas=True)

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
    def Upload(self, upload_dict=None):
        """Function: Upload()

        The list of dictionaries need to follow this convention:
        Rucio/Github: https://github.com/rucio/rucio/blob/master/lib/rucio/client/uploadclient.py#L71

        :param upload_dict: A list object with dictionaries
        :return result: 0 on success, 1 on failure

        """
        result = 1

        try:
            result = self._rucio_client_upload.upload(upload_dict)
        except NoFilesUploaded as e:
            print(e)
        except NotAllFilesUploaded as e:
            print(e)

        return result

    def DownloadDids(self, items, num_threads=2, trace_custom_fields={}):
        """Function: DownloadDids(...)

        Download from the Rucio catalogue by Rucio DIDs (or a list of them)

        :param items: A list or a dictionary of information what to download
        :param num_threads: Specify the number threads on the CPU, standard 2 (optional)
        :param trace_custom_fields: Customize downloads (Look at Rucio tutorials) (optional)
        :return result: A list of dictionaries of Rucio download result messages. If it fails: 1
        """
        result = 1
        #if a dictionary is handed over, we create a list of it.
        if isinstance(items, dict):
            items = [items]

        try:
            result = self._rucio_client_download.download_dids(items=items,
                                                       num_threads=num_threads,
                                                       trace_custom_fields=trace_custom_fields)
        except:
            result = 1

        return result

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
