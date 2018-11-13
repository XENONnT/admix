#Author: Boris Bauermeister
#Descriptions: Interface class to varous data format formats (locations, rucio, s3,...)

#import sys
#import datetime
#import os
import json

class Templater():
    
    def __init__(self):
        self.template_configuration_ = None
        self.config_path = None
        self.eval_list_ = {}
        
    def Config(self, config_path):
        #put in your favourite experimental configuration
        #to describe the rucio cataloge description
        self.config_path = config_path
        self.LoadConfig()
        
        
    def LoadConfig(self):
        with open(self.config_path) as f:
            self.template_configuration_ = json.load(f)
    
    def FindOccurrences(self, test_string, test_char):
        #https://stackoverflow.com/questions/13009675/find-all-the-occurrences-of-a-character-in-a-string
        return [i for i, letter in enumerate(test_string) if letter == test_char]
    
    def ExtractTagWords(self, test_string, beg_char, end_char):
        word_list = []
        char_beg = self.FindOccurrences(test_string, beg_char)
        char_end = self.FindOccurrences(test_string, end_char)
        for j in range(len(char_beg)):
            j_char_beg = char_beg[j]
            j_char_end = char_end[j]
            word = test_string[j_char_beg+1:j_char_end]
            if word not in word_list:
                word_list.append(word)
        return word_list
    
    def ExtractSplit(self, test_string, beg_char, end_char):
        split_list = []
        
        char_beg = self.FindOccurrences(test_string, beg_char)
        char_end = self.FindOccurrences(test_string, end_char)
        
        for j in range(len(char_beg)-1):
            j_char_beg = char_beg[j]
            j_char_end = char_end[j]
            if j_char_beg > j_char_end:
                j_char_beg = char_beg[j]
                j_char_end = char_end[j+1]
            
            split_symbol = test_string[j_char_beg+1:j_char_end]
            split_list.extend(split_symbol)
        return split_list
            
    def Eval(self, plugin=None, host=None, string=None):
        #Get data types (=keys)
        
        #Prepare the return:
        self.structure_ = None
        self.eval_list_ = {}
        
        self.types_ = list(self.template_configuration_.keys())
        
        if len(self.types_) == 0:
            print("Upload types are not defined")
            
        
        #Get the overall file structure from your configuration file (depends on experiment)
        
        for i_type in self.types_:
            if i_type != plugin:
                continue
            i_levels  = self.template_configuration_[i_type]
            if host in list(i_levels.keys()):
                self.structure_ = i_levels[host]
        
        if string==None:
            return 0
        
        if self.structure_ == None:
            #print("check your config file")
            return 0
        
        #Evaluate the string according the template:
        tag_words = self.ExtractTagWords(self.structure_, "{", "}")
        split_list = self.ExtractSplit(self.structure_, "}", "{")
       
        
        tag_words = list(reversed(tag_words))
        split_list= reversed(split_list)
        for i, i_split in enumerate(split_list):
            take = string.split(i_split)[-1]
            string = string.replace(i_split+take, "")
            self.eval_list_[tag_words[i]]=take
            #print(i, take, string)
        self.eval_list_[tag_words[-1]]=string
        
    
    def GetTypes(self):
        return self.types_
    
    def GetStructure(self, plugin=None, host=None):
        self.Eval(plugin=plugin, host=host, string=None)
        return self.structure_
    def GetTemplateEval(self, plugin=None, host=None, string=None):
        self.Eval(plugin=plugin, host=host, string=string)
        return self.eval_list_