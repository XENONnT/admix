# -*- coding: utf-8 -*-
import json
import os
from admix.helper import helper

from admix.interfaces.database import ConnectMongoDB
from admix.helper.decorator import Collector

#get Rucio imports done:
from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.destination import Destination
from admix.interfaces.keyword import Keyword
from admix.interfaces.templater import Templater
from admix.utils.naming import make_did

def list_file_replicas(run_number, dtype, hash, rse='UC_DALI_USERDISK'):

    db = ConnectMongoDB()
    rc = RucioSummoner(helper.get_hostconfig("rucio_backend"))

#    print("Looking for run "+str(run_number)+", data type "+dtype+", hash "+hash+", in rse="+rse)

    # checks if run is present in run database
    # this will improve the reaction speed in case the run is not existing
    # since we do not call Rucio commands
    cursor = db.GetRunByNumber(run_number)        
    if len(cursor)==0:
#        print("Error. Run not existing in database")
        return list()

    # build did
    did = make_did(run_number, dtype, hash)

    file_replicas = {}

    # check if the did esists in the given rse
    if rc.CheckRule(did, rse) != 'OK':
#        print("Error. Not found in this rse")
        return list()

    file_replicas = rc.ListFileReplicas(did,rse,localpath=True)

    return list(file_replicas.values())

