#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 20 08:52:03 2018

@author: sam
"""

from usb_dioSS import usb_erb24

import time
import numpy as np
import collections
import json

import KafkaInOut
import argparse

# this class logs disk_free, mem_free and cpu_used
# Still need to add hostname!
# no need for init or closing functions
class Relay():
    def __init__(self):
        try:
            self.relay=usb_erb24()
        except Exception as e:
            print('Could not find device: {}'.format(e))
            
    def test(self):
        return True # always returns true as it should always work
    
    def numsToBits(self,nums):
        bits=np.zeros(24,dtype=np.int)
        ch0,ch1,ch2,ch3=nums
        for i in range(8):
            bits[i]=np.binary_repr(ch0,8)[7-i]
            bits[8+i]=np.binary_repr(ch1,8)[7-i]
        for i in range(4):
            bits[16+i]=np.binary_repr(ch2,4)[3-i]
            bits[20+i]=np.binary_repr(ch3,4)[3-i]
        return bits
    
    def bitsToNums(self,bits):
        string=''
        for b in bits[:8]:
            string+='{}'.format(b)
        sstring=string[::-1]
        ch0=int(sstring,2)
        
        string=''
        for b in bits[8:16]:
            string+='{}'.format(b)
        sstring=string[::-1]
        ch1=int(sstring,2)
        
        string=''
        for b in bits[16:20]:
            string+='{}'.format(b)
        sstring=string[::-1]
        ch2=int(sstring,2)
        
        string=''
        for b in bits[20:]:
            string+='{}'.format(b)
        sstring=string[::-1]
        ch3=int(sstring,2)
        
        return [ch0,ch1,ch2,ch3]
            
            
    def askOutput(self,topic):
        ch0=self.relay.DIn(0)
        ch1=self.relay.DIn(1)
        ch2=self.relay.DIn(2)
        ch3=self.relay.DIn(3)
        bits=self.numsToBits([ch0,ch1,ch2,ch3])
    
        nu=int(np.round(time.time()*1000))
        systInfo=collections.OrderedDict()
        systInfo['@timestamp']=nu
        systInfo['bitvalue']=json.dumps(bits.tolist())
        systInfo['portnumb']=json.dumps((np.arange(24)+1).tolist())

        return systInfo

    def giveInput(self,topic, eventList):
        if (len(eventList)>0):
            js=eventList[-1]   # only use the last configuration meassage
            try:
                bitvalue=js['bitvalue']
                #print(bitvalue)
                nums=self.bitsToNums(bitvalue)
                for i in range(4):
                    self.relay.DOut(i,nums[i])
            except Exception as e:
                print('Probably a problem in the json: {}.'.format(e))
                
        return
    
    def close(self):
        return self.daq.h.close() # always works
    
# the code run if I run the program from the command line
if __name__ == '__main__':
    import IntervalRunner # watch out! this will get the asyncio loop!
    
    parser = argparse.ArgumentParser()
    parser.add_argument('interval',help='Logging interval')
#    parser.add_argument('-f','--filename',help="Log file name", default="0")
    parser.add_argument('-o','--kafka_topic',help='Kafka topic name', default='relay.log')
    
    args=parser.parse_args()
    
    interval=float(args.interval)
    if (interval<0.1): # some arbitrary speed limit imposed here.
        interval=0.1
    
    DD=Relay()
    kk=KafkaInOut.KafkaInOut()
    kk.setDevice(DD,'relay')
    
    todoList=[]
    
    finLogName,logger=kk.makeProducer('relay.log')   # need to create a visible object of logger to avoid premature closure..
    loggerFunc=lambda:kk.produceOutput(finLogName,logger)
    todoList.append((interval,loggerFunc))
    
    finConfigName,configer=kk.makeConsumer('relay.config','daqConfiger',{'auto.offset.reset':'earliest'},True,[1,1])
    configFunc=lambda:kk.consumeInput(finConfigName,configer)
    todoList.append((0.2,configFunc))
    
    
    IntervalRunner.doIt(todoList)
    


