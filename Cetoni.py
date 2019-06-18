#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 20 08:52:03 2018

@author: sam
"""

from qmixsdk import qmixbus,qmixpump
#from qmixbus import UnitPrefix, TimeUnit
#from qmixpump import VolumeUnit

import time
import numpy as np
import collections

import KafkaInOut
import argparse

# this class controls the Cetoni pump
# the constructor can be with or without calibration (default=False)
class Pump():
    def __init__(self,doCalibration=False): # calibration will move the plunger!
        try:
            self.bus=qmixbus.Bus()
            self.bus.open('vinnig','')
            self.bus.start()
            self.pump=qmixpump.Pump()
            self.pump.lookup_by_device_index(0)
            self.pump.clear_fault()
            self.pump.enable(True)
            self.pump.set_volume_unit(qmixbus.UnitPrefix.milli,qmixpump.VolumeUnit.litres)
            self.pump.set_flow_unit(qmixbus.UnitPrefix.milli,qmixpump.VolumeUnit.litres,qmixbus.TimeUnit.per_second)
            self.valve=self.pump.get_valve()

            if (doCalibration):
                self.pump.calibrate()
                timeout_timer=qmixbus.PollingTimer(10000)
                timeout_timer.wait_until(self.pump.is_calibration_finished,True)
            
        except:
            print('Could not init device')
            
    def doCalibration(self):
        self.pump.calibrate()
        timeout_timer=qmixbus.PollingTimer(10000)
        timeout_timer.wait_until(self.pump.is_calibration_finished,True)
        return
            
    def getInfo(self):
        systInfo=collections.OrderedDict()
        try:
            nu=int(np.round(time.time()*1000))
            systInfo['@timestamp']=nu
            systInfo['fill_level']=self.pump.get_fill_level()
            systInfo['is_pumping']=self.pump.is_pumping()
            systInfo['flow_rate']=self.pump.get_flow_is()
            systInfo['syringe']=self.pump.get_syringe_param()[0]
            systInfo['max_flow_rate']=self.pump.get_flow_rate_max()
            systInfo['max_volume']=self.pump.get_volume_max()
            systInfo['valve_state']=self.valve.actual_valve_position()
            systInfo['pump_enabled']=self.pump.is_enabled()
            systInfo['pump_fault']=self.pump.is_in_fault_state()

            return systInfo
        except Exception as e:
            print('Problem getting info from pump: {}'.format(e))
            return systInfo # return an empty dictionary if no configuration has been set => no events logged
    
    def test(self):
        return self.pump.is_enabled() # always returns true as it should always work
    
    
    def setSyringe(self,inner,piston):
        self.pump.set_syringe_param(inner,piston)
        
    def setFillLevel(self,level,flow):
        self.pump.set_fill_level(level,flow)
        
    def stopPumping(self):
        self.pump.stop_pumping()
    
    def makeFlow(self,flow):
        self.pump.generate_flow(flow)   
        
    def valveSwitch(self, newPosition):
        self.valve.switch_valve_to_position(newPosition)
    
    # incoming configuration should include 
    #           syringe information
    #           new desired fill level
    #           valve position
    def setInfo(self,commandList):
        foundNewConfig=False
        if (len(commandList)>0):
            for js in commandList:
                try:
                    configList=js['configuration']
                    for config in configList:
                        name,func=self.processOneConfig(config)
                        if (name):
                            if (not foundNewConfig): # found a new scanlist, reinitialise configuration
                                self.chanNames=[]
                                self.chanFuncs=[]
                                foundNewConfig=True
                            self.chanNames.append(name)
                            self.chanFuncs.append(func)
                except:
                    print('Probably a problem in the json.')
                
        return
    
    def close(self):
        self.pump.stop_pumping()
        self.pump.enable(False)
        return 
    
# the code run if I run the program from the command line
if __name__ == '__main__':
    import IntervalRunner # watch out! this will get the asyncio loop!
    
    parser = argparse.ArgumentParser()
    parser.add_argument('interval',help='Logging interval')
#    parser.add_argument('-f','--filename',help="Log file name", default="0")
    parser.add_argument('-o','--kafka_topic',help='Kafka topic name', default='daqLog')
    
    args=parser.parse_args()
    
    interval=float(args.interval)
    if (interval<0.1): # some arbitrary speed limit imposed here.
        interval=0.1
    
    PP=Pump(False)
    kk=KafkaInOut.KafkaInOut(args.kafka_topic,'tempOut')
    kk.setDevice(PP,'pump')
    
    drift=0.0027  # modify per routine as Intervalrunner depends on execution time object.
    todoList=[(interval-drift,kk.logResult),(0.3,kk.consumeEvents)]
    IntervalRunner.doIt(todoList)
    


