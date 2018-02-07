# -*- coding: utf-8 -*-

import logging
import rucio
from rucio.client.client import Client

from admix.runDB import xenon_runDB as XenonRunDatabase

class TestaDMIX():
    
    def __init__(self):
        
        self.rucio_client = Client()
        self.xrd = XenonRunDatabase.XenonRunDatabase()
    
    def PrintTester(self):
        print("Connection tester for aDMIX")
        print("Load the database:")
        self.xrd.LoadCollection()
        self.xrd.CreateQuery()
        cursor = self.xrd.GetCursor()
        print("Xenon runDB: LOADED [X]")
        print("       %s runs" % len(cursor) )
        
        print("Load the Rucio catalogue")
        rucio_ping = self.rucio_client.ping()
        rucio_account_whoami = self.rucio_client.whoami()
        print("Ping Rucio server: ", rucio_ping['version'])
        print("Whoami:")
        for key, value in rucio_account_whoami.items():
            print("INFO about {key} \t \t: {val}".format(key=key, val=value))
        
        