# -*- coding: utf-8 -*-

import logging
import datetime
import time

#import rucio
#from rucio.client.client import Client

#from admix.runDB import xenon_runDB as XenonRunDatabase

import os
from admix.tasks import helper
from admix.interfaces.database import DataBase, ConnectMongoDB
from admix.interfaces.rucioapi import ConfigRucioDataFormat, RucioAPI, RucioCLI, TransferRucio
from admix.interfaces.templater import Templater

class set_manuell_transfers():

    def __init__(self):
        print('Upload with mongoDB starts')

    def init(self):
        self.db = ConnectMongoDB()
        self.db.Connect()

        #Since we deal with an experiment, everything is predefine:
        self.exp_temp = Templater()
        self.exp_temp.Config( helper.get_hostconfig()['template'] )


    def run(self,*args, **kwargs):
        self.init()
        print("-------------------------")
        print("Task: Set up a transfer manually")

        #Decide if you select runs from argsparse or just take everythin:
        if 'run_beg' in helper.global_dictionary:
            run_beg = helper.global_dictionary['run_beg']
        else:
            run_beg = self.db.GetSmallest('number')

        if 'run_end' in helper.global_dictionary:
            run_end = helper.global_dictionary['run_end']
        else:
            run_end = self.db.GetLargest('number')

        #When you found your run numbers, you want to select timestamps from them
        #This allows to catch everything in between
        ts_beg = self.db.FindTimeStamp('number', int(run_beg) )
        ts_end = self.db.FindTimeStamp('number', int(run_end) )

        #Query an overview collection with the relevant information
        #According to the selected timestamps:
        self.db.Connect()
        self.db.SetProjection(projection={'number':True, 'name':True, '_id':True, 'start':True}, from_config=False)
        query = { '$and': [ {'start': {'$gte':ts_beg}}, {'start': {'$lte':ts_end}} ] }
        #collection = self.db.GetQuery(query, sort=[('name',-1)])
        collection = self.db.GetQuery(query)

        #Once you grabbed the overview collection, you want to
        #reset your mongodb projections to include the data field with
        #the locations:
        self.db.Connect()
        self.db.SetProjection(projection={'number':True, 'name':True, '_id':True, 'data':True, 'detector':True, 'start':True}, from_config=False)

        #Run through the overview collection:
        for i_run in collection:
            #Extract run number and name from overview collection
            r_name = i_run['name']
            r_number = i_run['number']
            #Pull the full run information (according to projection which is pre-defined) by the run name
            db_info = self.db.GetRunByName(r_name)[0]

            print("")
            print("")
            print("Attempt to change destination for")
            print(" -> run", r_number, "/", r_number)
            print("")


            for i_data in db_info['data']:
                host_        = None
                location_    = None
                status_      = None
                type_        = None
                destination_ = []
                rse_         = None

                if 'host' in i_data:
                    host_     = i_data['host']
                if 'location' in i_data:
                    location_ = i_data['location']
                if 'status' in i_data:
                    status_   = i_data['status']
                if 'type' in i_data:
                    type_     = i_data['type']
                if 'destination' in i_data:
                    destination_     = i_data['destination']
                if 'rse' in i_data:
                    rse_     = i_data['rse']

                #print(" -> ", host_, location_, status_, type_, rse_, destination_)

                #exclude some destination marker for now:
                if host_ == 'tsm-server' or host_ == "rucio-catalogue":
                    continue
                if type_ == 'processed':
                    continue

                #skip if folder not exists:
                if os.path.isdir(i_data['location']) == False:
                    continue

                #skip location if _tmp is found in one of the paths:
                tmp_occurence = False
                if os.path.isdir(i_data['location']):
                    ret_folder = []
                    ret_dirpath = []
                    ret_files = []
                    for (dirpath, dirnames, filenames) in os.walk(i_data['location']):
                        ret_dirpath.extend(dirpath)
                        ret_folder.extend(dirnames)
                        ret_files.extend(filenames)
                        break
                    for i_file in ret_files:
                        if i_file.find("_tmp")>=0:
                            tmp_occurence=True
                if tmp_occurence == True:
                    continue


                on_rucio = False
                for j_data in db_info['data']:
                    if j_data['host'] == 'rucio-catalogue' and j_data['type'] == type_:
                        on_rucio = True
                        print("on rucio check")
                        print("________")
                        print("rse->", self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='rse'))
                        print("type->", self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='type'))
                        print("destination (rucio)->", self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='destination'))
                        print("destination (dali)->", self.db.GetDataField(db_info['_id'], type=type_, host='dali', key='destination'))
                        #self.db.SetDataField(db_info['_id'], type=type_, host="rucio-catalogue", key="destination", value=[])
                        print("status->", self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='status'))
                        print("------------------------")

                    #manipulate data:

                print("Rucio:", on_rucio)

                if on_rucio == True:

                    test = input("Update specfic but on Rucio <y/n>")
                    if test == 'y':
                        destination_ = self.own_update(destination_)
                        self.db.SetDataField(db_info['_id'], type=type_, host="dali", key="destination", value=destination_)

                    continue
                else:
                    print("Run and update")
                    print("--------------")
                    print("type:", self.db.GetDataField(db_info['_id'], type=type_, host='dali', key='type'))
                    print("type:", self.db.GetDataField(db_info['_id'], type=type_, host='dali', key='location'))
                    print("------->Y<------")
                    test = input("Add dali/rucio-catalogue:UC_OSG_USERDISK:None destination <y/n>")
                    if test == 'y':
                        self.db.SetDataField(db_info['_id'], type=type_, host='dali',  key="destination", value=["rucio-catalogue:UC_OSG_USERDISK:None"], new=True)
                    test = input("Update specfic <y/n>")
                    if test == 'y':
                        destination_ = self.own_update(destination_)
                        self.db.SetDataField(db_info['_id'], type=type_, host="dali", key="destination", value=destination_, new=True)


                    print("no changes")

                continue


                #if on_dali == True and on_rucio == False:
                    #for i_data in db_info['data']:
                        #print("  -> ", i_data['type'], i_data['host'], i_data['location'], db_info['_id'])

                    ###prepare with a destination:
                    #print("CHANGE")
                    #self.db.WriteDestination(db_info['_id'], type=plugin, host='dali', destination="rucio-catalogue:UC_OSG_USERDISK:None")
                    ##self.db.WriteDestination(db_info['_id'], type=plugin, host='dali', destination="rucio-catalogue:NIKHEF_USERDISK:86400")
                    ##continue
                #else:
                    #print(" ----", type_, 'rucio-catalogue', ":")
                    ##print("->", self.db.GetDataField(db_info['_id'], type=type_, host=dest_host, key='destination'))
                    #print("rse->", self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='rse'))
                    #print("type->", self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='type'))
                    #print("destination->", self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='destination'))
                    #print("status->", self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='status'))

                #continue
                #print(" ----", type_, 'rucio-catalogue', ":")
                ##print("->", self.db.GetDataField(db_info['_id'], type=type_, host=dest_host, key='destination'))
                #print("rse->", self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='rse'))
                #print("type->", self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='type'))
                #print("destination->", self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='destination'))
                #print("status->", self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='status'))

                #if self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='rse') == ['UC_OSG_USERDISK:OK:None']:
                    #print("set status")
                    #self.db.SetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='status', value='transferred', new=True)
                    #print("check")
                    #print( self.db.GetDataField(db_info['_id'], type=type_, host='rucio-catalogue', key='status') )




                #if on_dali == False:# or on_rucio == False:
                    #continue


                    #else:
                        #rse = None


                #else:
                    #pass
                    ##print("nothing more to do")

            #if helper.global_dictionary['no_db_update'] == False:
                #self.db.UpdateDatafield(db_info['_id'], db_info['data'])
            #else:
                #print("Only testing, no updates are done")


        #end task
        return 1

    def own_update(self, destination_):
        allowed_rse = ["UC_OSG_USERDISK", "NIKHEF_USERDISK", "CCIN2P3_USERDISK"]
        print("")
        print("Do you want to set a destination for:")
        print(" -> Destination:", destination_)
        dec = input("[y/n]")
        if dec == "y":
            ptr = input("Destination/protocol (rucio-catalogue /):")
            if ptr == 'rucio-catalogue':
                print("RSEs are: ", allowed_rse)
                rse = input("type:")
                if rse not in allowed_rse:
                    rse = None
                rlt = input("Specify lifetime [s]")
                if int(rlt) == -1:
                    rlt=None

                new_dest = "{ptr}:{rse}:{rlt}".format(ptr=ptr, rse=rse, rlt=rlt)
                print("You want to assign:", new_dest)
                print("")
                print("Current destination:", destination_)
                overw = input("Do you want to overwrite the current destination? (y/n) ")
                if overw == "y":
                    destination_ = [new_dest]
                else:
                    destination_.append(new_dest)
                    print(destination_)
                    push_upload = input("Do you want to upload to this RSE? (y/n) ")
                    if push_upload == "y":
                        a, b = destination_.index(destination_[0]), destination_.index(new_dest)
                        destination_[b], destination_[a] = destination_[a], destination_[b]
                print("new destination:")
                print(destination_)
        return destination_

    def __del__(self):
        pass


