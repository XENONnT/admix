#This will be the frontend for Rucio

from __future__ import with_statement
import rucio
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
from rucio.common.exception import DataIdentifierAlreadyExists

import sys
import tempfile
import subprocess
import datetime
import os
import json

class ConfigRucioAPI():
    #read/config rucio interface
    def __init__(self):
        self.rucio_configuration_ = None
        self.types_ = []
        self.structure_ = {}
        self.eval_ = 0
        print("Init ConfigRucioAPI")
        
    def Config(self, config_path):
        #put in your favourite experimental configuration
        #to describe the rucio cataloge description
        self.config_path = config_path
        self.LoadConfig()
        self.Eval()
        
    def LoadConfig(self):
        with open(self.config_path) as f:
            self.rucio_configuration_ = json.load(f)
    
    def FindOccurrences(self, test_string, test_char):
        #https://stackoverflow.com/questions/13009675/find-all-the-occurrences-of-a-character-in-a-string
        return [i for i, letter in enumerate(test_string) if letter == test_char]
    
    def ExtractTagWords(self, test_string, beg_char, end_char):
        word_list = []
        char_beg = self.FindOccurrences(test_string, "{")
        char_end = self.FindOccurrences(test_string, "}")
        for j in range(len(char_beg)):
            j_char_beg = char_beg[j]
            j_char_end = char_end[j]
            word = test_string[j_char_beg+1:j_char_end]
            if word not in word_list:
                word_list.append(word)
        return word_list
                
    def Eval(self):
        #Get data types (=keys)
        self.types_ = list(self.rucio_configuration_.keys())
        
        if len(self.types_) == 0:
            print("Upload types are not defined")
            
        
        #Get the overall file structure from your configuration file (depends on experiment)
        self.structure_ = {}
        try: 
            for i_type in self.types_:
                i_levels  = self.rucio_configuration_[i_type].split("|->|")
                nb_levels = len(i_levels)
                
                #init the level at least once beforehand
                self.structure_[i_type] = {}
                for idx, i_level in enumerate(i_levels):
                    self.structure_[i_type]["L"+str(idx)] = {}
                
                #fill the levels with information:
                for idx, i_level in enumerate(i_levels):
                    if i_level.find('$C') == 0:
                        self.structure_[i_type]["L"+str(idx)]['type'] = "rucio_container"
                        self.structure_[i_type]["L"+str(idx)]['did'] = i_level.replace("$C", "")
                    if i_level.find('$D') == 0:
                        self.structure_[i_type]["L"+str(idx)]['type'] = "rucio_dataset"
                        self.structure_[i_type]["L"+str(idx)]['did'] = i_level.replace("$D", "")
                    
                    self.structure_[i_type]["L"+str(idx)]['tag_words'] = self.ExtractTagWords(i_level, "{", "}")
            self.eval_ = 1
        except:
            print("Evaluation failed")

    def GetTypes(self):
        return self.types_
    def GetStructure(self):
        return self.structure_
        
class RucioAPI():
    def __init__(self):
        print("Init RucioAPI")