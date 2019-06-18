#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 20 08:52:03 2018

@author: sam
"""

from usb_2400 import usb_2408, Thermocouple

import time
import numpy as np
import collections

import KafkaInOut
import argparse

# this class logs disk_free, mem_free and cpu_used
# Still need to add hostname!
# no need for init or closing functions
class Daq():
    def __init__(self):
        try:
            self.daq=usb_2408()
            self.chanNames=[]
            self.chanFuncs=[]
        except:
            print('Could not find device')
            
    # rate=10 is 30S/s, other good values are 8, 12, 13, 14, 15 with higher number=> slower
    def Temperature(self, tc_type, channel,rate=10):
        tc = Thermocouple()
        # Read the raw voltage (Mode = 4, Range = +/- .078V, Rate = 60S/s)
        (value, flag) = self.daq.AIn(channel, 4, 8, rate)
        if flag & 0x80:
          print('TC open detected.  Check wiring on channel', channel)
          return -1
        # Apply calibration offset from Gain Table (EEPROM) address 0x0130 (slope) and 0x0138 (offset)
        value = value*self.daq.Cal[9].slope + self.daq.Cal[9].intercept
        # Convert the corrected valued to a voltage (Volts)
        tc_voltage = (value * 2. * 0.078125) / 16777216.
        # Correct the CJC Temperature by the CJCGradiant for the appropriate channel
        cjc = self.daq.CJC()
        CJC_temp = cjc[channel//4] - self.daq.CJCGradient[channel]
        # Calculate the CJC voltage using the NIST polynomials and add to tc_voltage in millivolts
        tc_mv = tc.temp_to_mv(tc_type, CJC_temp) + tc_voltage*1000.
        # Calcualate actual temperature using reverse NIST polynomial.
        return tc.mv_to_temp(tc_type, tc_mv)
    
    def Voltage(self,channel,mode,gain,rate=10):
        value,flag=self.daq.AIn(channel,mode,gain,rate)
        data=int(value*self.daq.Cal[gain].slope + self.daq.Cal[gain].intercept)
        return self.daq.volts(gain,data)
    
    def getInfo(self):
        if (len(self.chanNames)>0):
            nu=int(np.round(time.time()*1000))
            systInfo=collections.OrderedDict()
            systInfo['@timestamp']=nu
            for i in range(len(self.chanNames)):
                systInfo[self.chanNames[i]]=self.chanFuncs[i]()

            return systInfo
        else:
            return {} # return an empty dictionary if no configuration has been set => no events logged
    
    def test(self):
        return True # always returns true as it should always work
    
    def processOneConfig(self,config):
        try:
            name=config['Name']
            measType=config['Measurement']
            channel=config['Channel']
            rate=config['Rate']
            if (measType=='Temperature'):
                typeTC=config['Type']
                return (name,lambda:self.Temperature(typeTC,channel,rate))
            elif (measType=='Voltage'):
                mode=config['Mode']
                gain=config['Gain']
                return (name,lambda:self.Voltage(channel,mode,gain,rate))
        except:
            print('Problem parsing configuration: {}'.format(config))
            return None,None
            
    
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
        return self.daq.udev.close() # always works
    
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
    
    DD=Daq()
    kk=KafkaInOut.KafkaInOut(args.kafka_topic,'tempOut')
    kk.setDevice(DD,'daq')
    
    drift=0.0027  # modify per routine as Intervalrunner depends on execution time object.
    todoList=[(interval-drift,kk.logResult),(0.3,kk.consumeEvents)]
    IntervalRunner.doIt(todoList)
    


