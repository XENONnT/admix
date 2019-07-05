#Author: Boris Bauermeister
#Descriptions: Interface class to evaluate the destination

#import sys
#import datetime
#import os
import re

class Destination():

    def __init__(self):
        self._destination = None #holds the information about what is to do at the destination
        self._host        = None #holds the host where aDMIX is running
        self._origin      = None #holds the information from which locations the data destination

        self._rucio_rule  = []
        self._template = {
                            "rucio":"^rucio-catalogue:(\w+):(\w+)",
                            "rucio-download":"(\w+)",
                            "file":"^file:(\w+):(\w+)",
                         }


    def RunDestination(self):
        #print(self._host, self._origin, self._destination)

        self.__host_is_origin()
        self.__prepare_destination()

        #print(self._rucio_rule, self._host_is_origin)

        if self._host_is_origin == True and len(self._rucio_rule) > 0:
            #you are up for rucio uploads based on:
            # - your destination
            # - your host
            # --> upload host to destination with possible rules!
            return self._rucio_rule

        else:
            return []

    def EvalDestination(self, host, origin, destination):
        self._destination = destination
        self._origin      = origin
        self._host        = host
        self._rucio_rule  = []

        return self.RunDestination()

    def __prepare_destination(self):
        #make a list out of the destination just in case:
        if isinstance(self._destination, list) == False:
            self._destination = [self._destination]


        #evaluate the list of destinations:
        for i_dest in self._destination:
            #you might pass a None destination at
            #some point. But you want to ignore this
            if i_dest == None:
                continue
            if bool(re.search(self._template['rucio'], i_dest)):
                rc_dict={}
                rc_dict['protocol']='rucio-catalogue'
                rc_dict['rse'], rc_dict['lifetime'] = re.search(self._template['rucio'], i_dest).group(1,2)
                if len(self._rucio_rule) == 0:
                    rc_dict['upload'] = True
                else:
                    rc_dict['upload'] = False
                self._rucio_rule.append(rc_dict)
            if bool(re.search(self._template['file'], i_dest)):
                print(i_dest)
                rc_dict = {}
                rc_dict['protocol'] = 'file'
                rc_dict['dir'], rc_dict['info'] = re.search(self._template['file'], i_dest).group(1, 2)
                #if len(self._rucio_rule) == 0:
                #    rc_dict['upload'] = True
                #else:
                #    rc_dict['upload'] = False
                self._rucio_rule.append(rc_dict)

    def __host_is_origin(self):
        self._host_is_origin = False
        if self._host == self._origin:
            self._host_is_origin = True

