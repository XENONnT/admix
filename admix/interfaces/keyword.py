#Author: Boris Bauermeister
#Descriptions: A class to manage key words in strings

import datetime

class Keyword():

    def __init__(self):
        self._template = []

    def SetTemplate(self, template):
        #A template is defined as a dictionary with keywords such as:
        # template = {'key1': 'keyword_content1', 'key2': 'keyword_content2', 'key3': 'keyword_content3', ... }
        # Based on this template, this class try to fill up the keyword string(s)
        self._template.append(template)

    def ResetTemplate(self):
        #If necessary you can reset all template dictionaries again
        self._template = []

    def SetString(self, string):
        #this is a string with which needs to be filled up with content from keys.
        pass

    def CompleteTemplate(self, template):
        #Fill a template dictionary with the help from CompleteKeywords()
        for key, val, in template.items():
            val = self.CompleteKeywords(val)
        return template

    def CompleteKeywords(self, val):
        if 'tag_words' not in val:
            return val
        if 'did' not in val:
            return val

        did = val['did']
        #determine the tags which need input information
        for i_tag in val['tag_words']:
            #try to find it in the self._template:
            for i_template in self._template:
                if i_tag in i_template:
                    did= did.replace("{"+i_tag+"}", str(i_template[i_tag]))
            #elif self._eval_db_info(i_tag, db_info) != None:
                #did= did.replace("{"+i_tag+"}", self._eval_db_info(i_tag, db_info))

        val['did'] = did
        return val


    def _eval_db_info(self, key, db_info):
        if key=="detector":
            return db_info['detector']
        elif key=="science_run":
            sr = self._get_science_run( db_info['start'] )
            if sr != -1:
                return sr
            else:
                return "{"+science_run+"}"
        else:
            return None

    def _get_science_run(self, timestamp ):
        #Evaluate science run periods:

        #1) Change from sc0 to sc1:
        dt0to1 = datetime.datetime(2017, 2, 2, 17, 40)

        #Evaluate the according science run number:
        if timestamp <= dt0to1:
            science_run = "000"
        elif timestamp >= dt0to1:
            science_run = "001"
        else:
            science_run = -1
        return science_run
