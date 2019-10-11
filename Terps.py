#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 20 08:52:03 2018

@author: sam
"""
#import asyncio
import serial
import time
import sys
import numpy as np
import collections

import KafkaInOut
import argparse

# this class logs disk_free, mem_free and cpu_used
# Still need to add hostname!
# no need for init or closing functions
class TerpsLogger():
    
    def __init__(self,portName):
        self.port=serial.Serial(portName,9600,timeout=2)
        
    
    def getInfo(self):
        systInfo=collections.OrderedDict()  # if getting data fails, empty returned.
        try:
            RR=self.port.readline()
#            print(RR)
            B=float(RR.strip())
            nu=int(np.round(time.time()*1000))
            
            systInfo['@timestamp']=nu
            systInfo['Pressure']=B
        except:
            e = sys.exc_info()[0]
            print('Failed reading TERPS: {}'.format(e))
        
        # OrderedDict used as this implies that order of keys that will be written
        # to file, will always be in the same order as specified here!
        #time.sleep(0.3)
        return systInfo
    
    def test(self):
        return self.port.isOpen() # always returns true as it should always work
    
    def setInfo(self,commands):
        return False # always false as nothing can be piloted
    
    def close(self):
        return self.port.close() # always works
    
    
    
# the code run if I run the program from the command line
if __name__ == '__main__':
    import IntervalRunner # watch out! this will get the asyncio loop!
    
    parser = argparse.ArgumentParser()
    parser.add_argument('interval',help='Logging interval')
#    parser.add_argument('-f','--filename',help="Log file name", default="0")
    parser.add_argument('-o','--kafka_topic',help='Kafka topic name', default='pressureLog')
    
    args=parser.parse_args()
    
    interval=float(args.interval)
    if (interval<0.1): # some arbitrary speed limit imposed here.
        interval=0.1
    
    TT=TerpsLogger('/dev/ttyUSB0')
    kk=KafkaInOut.KafkaInOut()
    kk.setDevice(TT,'terps')
    
    todoList=[]
    
    finLogName,logger=kk.makeProducer('terps.log')   # need to create a visible object of logger to avoid premature closure..
    loggerFunc=lambda:kk.produceOutput(finLogName,logger)
    drift=0.  # modify per routine as Intervalrunner depends on execution time object.
    todoList.append((interval-drift,loggerFunc))
    
    IntervalRunner.doIt(todoList)
    


