import logging
import tempfile
import subprocess
import datetime
import os
from admix.helper.decorator import Collector

@Collector
class RucioCLI():

    def __init__(self):
        print("Init the commandline module")
        self.rucio_version = None
        self.rucio_account = None
        self.rucio_host = None

        self.print_to_screen = False
        self.rucioCLI_enabled = False

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
            self.rucioCLI_enabled = True
        except:
            self.config = ""
            self.rucioCLI_enabled = False

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
                    #self.logger("InitProxy successfull for {host}".format(host=host))
                    #self.logger("Ticket expires at {date}".format(date=str_expire_date))


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

        status = 'OK'
        status_msg = []
        for i_msg in msg:
            if i_msg.find("ERROR")>= 0:
                status = "ERROR"
            status_msg.append(i_msg)

        return status, status_msg
