#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 20 08:52:03 2018

@author: sam
"""

import propar 

import time
import numpy as np
import collections

import KafkaInOut

class Bronkhorst():
    def __init__(self,address):
        try:
            self.flowcontroller=propar.instrument(address)
            self.checkControlMode()
            self.capacity=self.flowcontroller.readParameter(21)
            self.units=self.flowcontroller.readParameter(129)
            self.fluidName=self.flowcontroller.readParameter(25)
            if (self.capacity==None):
                print('Problem getting the capacity. Trying once more')
                self.flowcontroller=propar.instrument(address)
                self.capacity=self.flowcontroller.readParameter(21)
                self.units=self.flowcontroller.readParameter(129)
                self.fluidName=self.flowcontroller.readParameter(25)
        except Exception as e:
            print('Could not find device: {}'.format(e))
            
    def test(self):
        return True # always returns true as it should always work
    
    def measureFlow(self):
        #self.capacity=self.flowcontroller.readParameter(21)
        #print(self.capacity)
        meter=self.flowcontroller.readParameter(8)
        meas=meter/32000.0*self.capacity
        return meas
    
    def measureValve(self):
        val=self.flowcontroller.readParameter(55)
        valF=val/16777215.0*100 # output in %
        return valF
    
    def measureTemp(self):
        val=self.flowcontroller.readParameter(142)
        return val
    
    def getSetpoint(self):
        val=self.flowcontroller.readParameter(9)
        setp=val/32000.0*self.capacity
        return setp
    
    def setSetpoint(self,setp):
        intSetpoint=int(setp/self.capacity*32000)
        self.flowcontroller.writeParameter(9,intSetpoint)
        
    def checkControlMode(self):
        cm=self.flowcontroller.readParameter(12)
        if (cm!=0):
            print('Need to change control mode')
            self.flowcontroller.writeParameter(12,0)
    
    def askOutput(self,topic):
        flow=self.measureFlow()
        valve=self.measureValve()
        #temp=self.measureTemp()
        setp=self.getSetpoint()
    
        nu=int(np.round(time.time()*1000))
        systInfo=collections.OrderedDict()
        systInfo['@timestamp']=nu
        systInfo['flowrate']=flow
        systInfo['setpoint']=setp
        systInfo['valve']=valve
        #systInfo['temperature']=temp  # does not seem very usefull

        return systInfo

    def giveInput(self,topic, eventList):
        if (len(eventList)>0):
            js=eventList[-1]   # only use the last configuration meassage
            try:
                setp=js['setpoint']
                #print('setpoint : {}'.format(setp))
                self.setSetpoint(setp)
            except Exception as e:
                print('Problem setting the bronkhorst: {}.'.format(e))
                
        return
    
    def close(self):
        del self.flowcontroller
        return True # always works
    
# the code run if I run the program from the command line
if __name__ == '__main__':
    import IntervalRunner # watch out! this will get the asyncio loop!

    interval=0.5
    
    DD=Bronkhorst('/dev/ttyUSB1')
    kk=KafkaInOut.KafkaInOut()
    kk.setDevice(DD,'bronkhorst')
    
    todoList=[]
    
    finLogName,logger=kk.makeProducer('bronkhorst.log')   # need to create a visible object of logger to avoid premature closure..
    loggerFunc=lambda:kk.produceOutput(finLogName,logger)
    todoList.append((interval,loggerFunc))
    
    finConfigName,configer=kk.makeConsumer('bronkhorst.config','daqConfiger',{'auto.offset.reset':'earliest'},True,[1,1])
    configFunc=lambda:kk.consumeInput(finConfigName,configer)
    todoList.append((0.1,configFunc))
    
    
    IntervalRunner.doIt(todoList)
    


