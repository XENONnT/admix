# -*- coding: utf-8 -*-

#import rucio
#from rucio.client.client import Client

#from admix.runDB import xenon_runDB as XenonRunDatabase

class dummy():

    def __init__(self):
        print('dummy starts')

    def run(self,*args, **kwargs):
        print("run dummy")
        print(args)
        print(kwargs)

    def __del__(self):
        print( 'dummy stop')
