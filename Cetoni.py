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
    
    def askOutput(self,theTopic):
        if ('log' in theTopic):
            res=self.getInfo()
            return res
        else:
            print('Asking for other output than log: {}'.format(theTopic))
    
    def giveInput(self,theTopic,theList):
        if ('command' in theTopic):
            self.doCommands(theList)
        elif ('config' in theTopic):
            self.doConfig(theList)
        else:
            print('Received unkown input from topic: {}'.format(theTopic))
            
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
        
    # should act on all commands received, as we won't look at 'historical' data, only live feed
    def doCommands(self,commandList):
        for comm in commandList:
            try:
                theCommand=comm["command"]
                if (theCommand=='set_fill_level'):
                    theParams=comm["params"]
                    self.setFillLevel(theParams["level"],theParams["flow"])
                elif (theCommand=='make_flow'):
                    theParams=comm["params"]
                    self.makeFlow(theParams["flow"])
                elif (theCommand=='stop_pumping'):
                    self.stopPumping()
                else:
                    print('Did not understand the command: {}'.format(theCommand))
            except:
                print('Probably a problem in the command json')
        return

    # incoming configuration should include 
    #           syringe information
    #           valve position
    def doConfig(self,configList):
        # should only look at last configuration command
        js=configList[-1]
        try:
            syr=js["syringe_id"]
            pis=js["piston_stroke"]
            self.setSyringe(syr,pis)
            print('Successfully set the syringe: {} and {}'.format(syr,pis))
            val=js["valve_pos"]
            self.valveSwitch(val)
        except:
            print('Probably a problem in the json.')
                
        return
    
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
    kk=KafkaInOut.KafkaInOut()
    kk.setDevice(PP,'cetoni')
    
    todoList=[]
    
    finLogName,logger=kk.makeProducer('cetoni.log')   # need to create a visible object of logger to avoid premature closure..
    loggerFunc=lambda:kk.produceOutput(finLogName,logger)
    drift=0.0027  # modify per routine as Intervalrunner depends on execution time object.
    todoList.append((interval-drift,loggerFunc))
    
    finConfigName,configer=kk.makeConsumer('cetoni.config','cetoniConfiger',{'auto.offset.reset':'earliest'},True,[1,1])
    configFunc=lambda:kk.consumeInput(finConfigName,configer)
    drift=0  # modify per routine as Intervalrunner depends on execution time object.
    todoList.append((0.3-drift,configFunc))

    finCommandName,commander=kk.makeConsumer('cetoni.command','cetoniCommander',{'auto.offset.reset':'latest'},True,[1,1])
    commandFunc=lambda:kk.consumeInput(finCommandName,commander)
    drift=0  # modify per routine as Intervalrunner depends on execution time object.
    todoList.append((0.1-drift,commandFunc))
    
    IntervalRunner.doIt(todoList)
    


