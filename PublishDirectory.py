#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 14:39:43 2019

@author: sam
"""

import PublishJson
import json
import glob
import os.path
import time
#import argparse

class TimedPublisher():
    def __init__(self,connectParamsFile='Vinnig/KafkaConnectionSettings.json'):
        self.publisher=PublishJson.Publisher()
        self.publisher.setConnection(connectParamsFile)
        
    def analyseFolder(self,dirName):
        filenames=glob.glob(os.path.join(os.path.abspath(dirName),"*.json"))
        filenames.sort()
        waitTimes=[]
        jsonList=[]
        for fil in filenames:
            if (self.publisher.testJson(fil)):
                with open(fil) as f:
                    js=json.load(f)
                    
                if ('waittime' in js.keys()):
                    wt=js['waittime']
                    #print('Waittime for {} is {}'.format(fil,wt))
                    waitTimes.append(wt)
                else:
                    waitTimes.append(0)
                jsonList.append(fil)
                
        return jsonList,waitTimes
    
    def publishFolder(self,dirName):
        jList,wList=self.analyseFolder(dirName)
        for i in range(len(jList)):
            time.sleep(wList[i])
            self.publisher.readAndPublish(jList[i])
            
            
# the code run if I run the program from the command line
if __name__ == '__main__':
#    parser = argparse.ArgumentParser()
#    parser.add_argument('jsonDir',help='Json directory to upload')
    
#    args=parser.parse_args()
    
    PP=TimedPublisher()
    PP.publishFolder("/data/ULB/Source Code/KafkaLogging/json/InjectLiquid")