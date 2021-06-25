#!/usr/bin/python3
# -*- coding: utf-8 -*-

import mmap
import os
import signal
from time import sleep
import json
import sys

JSON_Datalist = {
        "schema":{"attributes": [{"attributeName":"content", 
                                  "attributeType":"text"}, 
                                 {"attributeName":"payload",
                                  "attributeType":"list"}]},
        "fields": [{"value":"test1"}, 
                   {"value":[{"attributeName":"content",
                              "start":0,
                              "end":4,
                              "key":"test",
                              "value":"test",
                              "tokenOffset":0}]}]}
inputFullPathFileName = sys.argv[1]
outputFullPathFileName = sys.argv[2]
my_length = 0
n2one = True
tag_wait = True # initiate state for N:1
tag_output = 'w'
tag_input = ''

class TupleOperator:
    def __init__(self):
        #hands shaking step
        self.tuple_dict = {}
        self.output_tuple_dict = {}
        self.javapid = -1
        self.f_input = open(inputFullPathFileName, 'r+b')
        self.map_input = mmap.mmap(self.f_input.fileno(),0)
        self.pythonpid = os.getpid()
        self.map_input.seek(0)
        self.javapid = int(self.map_input.readline())
        
        self.f_output = open(outputFullPathFileName,'r+b')
        self.map_output = mmap.mmap(self.f_output.fileno(), 0)
        self.map_output.seek(0)
        self.map_output.write(bytes(str(self.pythonpid)+"\n", 'utf-8'))
        os.kill(self.javapid, signal.SIGUSR2)

    def string_2_dict(self, string):
        self.tuple_dict = json.loads(json.dumps(str(string)))
        
    def add_field(self, attrName, attrType, value):
        #add schema
        attr = {"attributeName":attrName, "attributeType":attrType}
        (self.output_tuple_dict['schema']['attributes']).append(attr)
        #add field
        field = {"value": value}
        (self.output_tuple_dict['fields']).append(field)
        
    def get_valueByAttribute(self, attrname):
        #print(self.tuple_dict['schema'])
        for att,v in zip(self.tuple_dict['schema']['attributes'], self.tuple_dict['fields']):
            for attname,vv in att.items():
                if (vv == attrname):
                    return v['value']
        return None
    
    def read_input(self):
        global tag_input
        global tag_output
        self.map_input.seek(10)
        tag_input = self.map_input.readline().rstrip()
        if(tag_input != b'0'):
            textlen_input = tag_input
            self.map_input.seek(20)
            content_input = self.map_input.read(int(textlen_input))
            self.tuple_dict = json.loads(content_input.decode('utf-8'))
            #Here tag_input actually is the length of text. After extracting the length information,
            
            tag_input = 't'
        
    def write_output(self):
        global tag_input
        global tag_output
        if (tag_output == 't'):
#            self.output_tuple_dict = self.tuple_dict
            content_output = json.dumps(self.output_tuple_dict)
            tag_output = str(len(content_output))
            self.map_output.seek(10)
            textlen_output = len(content_output)
            self.map_output.write(bytes(str(textlen_output),'utf-8'))
            self.map_output.seek(20)
            self.map_output.write(content_output.encode('utf-8'))
            tag_output = 't'

        if (tag_output == '0'):
            self.map_output.seek(10)
            self.map_output.write('0'.encode('utf-8'))
            
        if (tag_output == 'w'):
            self.map_output.seek(10)
            self.map_output.write('w'.encode('utf-8'))

        self.tuple_dict = {}
        os.kill(self.javapid, signal.SIGUSR2)
        
    def user_defined_function(self):
        #User should implement this function
        pass

    def do_sig(self):
        self.read_input()
        ##############################################################
        ###user defined function here
        self.user_defined_function()
        ####################################################
        ## output Part
        self.write_output()
    
    def get_fieldvalue(self, field):
        value = self.tuple_dict['schema']['attributes']
        return value

    def onsignal_usr2(self, a,b):
        self.do_sig()
        
    def close(self):
        self.map.close()
        self.map_output.close()

#This is a demo User define function, user need to implement it.
class UserTupleOperator(TupleOperator):
    def user_defined_function(self):
        global n2one
        global tag_output
        global my_length
        n2one = True
        if (n2one == True):
            #This demo will compute the total length of field content for all tuple 
            if (tag_input == b'0' and tag_output == 't'):
                #send end signal null
                tag_output = '0'
            if (tag_input == b'0' and tag_output == 'w'):
                #send signal to write tuple
                tag_output = 't'
                attrType =  "text"
                value = my_length
                new_attrName = "length"
                self.add_field(new_attrName, attrType, value)
            if (tag_input == 't' and tag_output == 'w'):
                #send signal to wait
                tag_output = 'w'
                #do caculation
                attrName = "content"
                attrType = "text"
                            
                value = len(self.get_valueByAttribute(attrName))
                self.output_tuple_dict = self.tuple_dict
                
                my_length = my_length + value
                    
        if(n2one == False):
            #we need to construct a output dict
            #this demo will compute the length of field "content"
            if (tag_output == 'w'):
                tag_output = 't'
            if (tag_input == 't'):
                tag_output = 't'
                #send signal to write tuple
                self.output_tuple_dict = self.tuple_dict
                attrName = "content"
                attrType = "text"
                value = len(self.get_valueByAttribute(attrName))
                new_attrName = "length"
                self.add_field(new_attrName, attrType, value)
                
            if (tag_input == b'0'):
                #send signal of null
                tag_output = '0'

def main():
    userTupleOperator = UserTupleOperator()
    signal.signal(signal.SIGUSR2, userTupleOperator.onsignal_usr2)

    while 1:
        sleep(1)
								
if __name__ == "__main__":
    # execute only if run as a script
    main()
