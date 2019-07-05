"""
.. module:: rucio_cli
   :platform: Unix
   :synopsis: A legacy wrapper for the Rucio command line interface (CLI)

.. moduleauthor:: Boris Bauermeister <Boris.Bauermeister@gmail.com>

"""

import logging
import tempfile
import subprocess
import datetime
import os
from admix.helper.decorator import Collector

@Collector
class RucioCLI():

    def __init__(self):
        self.rucio_version = None
        self.rucio_account = None
        self.rucio_host = None

    # Here comes the backend configuration part:
    def SetConfigPath(self, config_path):
        self.rucio_cli_config = config_path

    def SetHost(self, host):
        self.rucio_host = host

    def SetProxyTicket(self, ticket_path):
        self.rucio_ticket_path = ticket_path

    def SetRucioAccount(self, account):
        self.rucio_account = account

    def ConfigHost(self):
        try:
            self.config = self.GetConfig().format(rucio_account=self.rucio_account, x509_user_proxy=self.rucio_ticket_path)
        except:
            print("Can not init the Rucio CLI")
            print("-> Check for your Rucio installation")
            exit(1)

    def Alive(self):
        print("RucioCLI alive")
    #Finished the backend configuration for RucioCLI

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



    def Whoami(self):
        """RucioCLI:Whoami
        Results a dictionary to identify the current
        Rucio user and credentials.
        CLI call requires to parse return string (msg)
        into a dictionary first.
        """
        upload_string = "rucio whoami"
        upload_string = self.config + upload_string
        msg, err = self.doRucio(upload_string)

        dict_msg = {}
        for i_msg in msg:
            i_msg = i_msg.replace(" ", "")
            key, value = i_msg.split(":", 1)
            dict_msg[key] = value
        return dict_msg

    def ListDidRules(self, scope, dname):
        """List rules by scope and name from the command line
        call.
        """

        pass
        #upload_string = f"rucio list-rules {scope}:{dname}"
        #upload_string = self.config + upload_string
        #msg, err = self.doRucio(upload_string)

        dict_msg = {}
        ...


    def CliUpload(self, method=None, upload_dict={}):

        upload_string = ""
        if method == "upload-folder-with-did":
            upload_string = "rucio upload --rse {rse} --scope {scope} {scope}:{did} {datasetpath}\n".format(rse=upload_dict['rse'],scope=upload_dict['scope'],did=upload_dict['did'], datasetpath=upload_dict['upload_path'])

        #cliupload:
        upload_string = self.config + upload_string

        print(upload_string)
        msg, err = self.doRucio(upload_string)

        status = 'OK'
        status_msg = []
        for i_msg in msg:
            if i_msg.find("ERROR")>= 0:
                status = "ERROR"
            status_msg.append(i_msg)

        return status, status_msg
