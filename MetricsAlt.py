#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 20 08:52:03 2018

@author: sam
"""
#import asyncio
import psutil
import time
import numpy as np
import collections

import KafkaInOutConfig
import argparse

# this class logs disk_free, mem_free and cpu_used
# Still need to add hostname!
# no need for init or closing functions
class MetricLogger():
    def getInfo(self):
        nu=int(np.round(time.time()*1000))
        ff=(int(psutil.disk_usage(".").free/1073741824 * 100))/100
        cc=psutil.cpu_percent()
        mm=(int(psutil.virtual_memory().available/(1024*1024*1024) * 100))/100
        systInfo=collections.OrderedDict()
        systInfo['@timestamp']=nu
        systInfo['free_space_gb']=ff
        systInfo['cpu_used_perc']=cc
        systInfo['free_memory_gb']=mm
        # OrderedDict used as this implies that order of keys that will be written
        # to file, will always be in the same order as specified here!
        #time.sleep(0.3)
        return systInfo
    
    def test(self):
        return True # always returns true as it should always work
    
    def doCommands(self,commandList):
        if (len(commandList)>1):
            print('Received more than 1 event in command: {}'.format(len(commandList)))
            print('Latest result is: {}'.format(commandList[-1]))
        else:
            #print('Received the following: {}'.format(commandList[0]))
            js=commandList[0]
            print('received a: {}'.format(type(js)))
            try:
                configList=js['configuration']
                for config in configList:
                    name=config['Name']
                    measType=config['Measurement']
                    print('New config with name {} and type {}'.format(name,measType))
            except:
                print('Probably a problem in the json.')
                
        return
    
    def doConfig(self,configList):
        if (len(configList)>1):
            print('Received more than 1 event in config: {}'.format(len(configList)))
            print('Latest result is: {}'.format(configList[-1]))
        else:
            #print('Received the following: {}'.format(commandList[0]))
            js=configList[0]
            print('received a: {}'.format(type(js)))
            try:
                configList=js['configuration']
                for config in configList:
                    name=config['Name']
                    measType=config['Measurement']
                    print('New config with name {} and type {}'.format(name,measType))
            except:
                print('Probably a problem in the json.')
                
        return
    
    def close(self):
        return True # always works
    
# the code run if I run the program from the command line
if __name__ == '__main__':
    import IntervalRunner # watch out! this will get the asyncio event loop!
    
    parser = argparse.ArgumentParser()
    parser.add_argument('interval',help='Logging interval')
#    parser.add_argument('-f','--filename',help="Log file name", default="0")
    parser.add_argument('-o','--log_topic',help='Kafka Logging topic name', default='metricOutput')
    parser.add_argument('-i','--command_topic',help='Kafka Command topic name', default='metricInput')
    
    args=parser.parse_args()
    
    interval=float(args.interval)
    if (interval<0.1): # some arbitrary speed limit imposed here.
        interval=0.1
    
    MM=MetricLogger()
    kk=KafkaInOutConfig.KafkaInOutConfig()
    kk.setDevice(MM,'metrics')
    kk.setCommandChannel('metrics.command','metricsalt.command')
    kk.setConfigChannel('metrics.config','metricsalt.config')
    kk.setLogChannel('metrics.log')
    
    drift=0.0027  # modify per routine as Intervalrunner depends on execution time object.
    todoList=[(interval-drift,kk.logResult),(0.1,kk.checkCommands),(0.3,kk.checkConfig)]
    IntervalRunner.doIt(todoList)
    


