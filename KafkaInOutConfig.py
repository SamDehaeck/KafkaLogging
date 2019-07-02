#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  6 15:30:43 2019

@author: sam
"""

from confluent_kafka import Producer,Consumer
import confluent_kafka.admin as admin
import json
import os.path
import random

class KafkaInOutConfig():
    def __init__(self,connectParamsFile='Vinnig/KafkaConnectionSettings.json',connectParams=None, topicBasename=''):
        if (connectParams==None):  # then read the configuration file. Location is relative to home directory
            home = os.path.expanduser("~")
            filename=os.path.join(home,connectParamsFile)
            with open(filename) as f:
                js=json.load(f)
                connectParams=js['connectParams']
                topicBasename=js['topicBasename']
        
        self.kafka_admin = admin.AdminClient(connectParams)
        self.topics=self.kafka_admin.list_topics().topics
        self.connectParams=connectParams
        self.topicBasename=topicBasename
        self.device=None
        self.deviceName='Unknown'
        
    # not a public method
    def makeProducer(self,TopicOut,topicParams=[1,1]):
        # setup producer
        self.topic_out=self.topicBasename+'.'+TopicOut
        
        if not (self.topic_out in self.topics):
            print('Should create Topic: {}'.format(self.topic_out))
            new_topic = admin.NewTopic(self.topic_out, topicParams[0],topicParams[1])
            self.kafka_admin.create_topics([new_topic,])
            
        return Producer(self.connectParams)
        
    # not a public method
    def makeConsumer(self,TopicIn,consumerID,topicConfig={'auto.offset.reset':'earliest'},topicParams=[1,1]):
        # Set up consumer
        self.topic_in=self.topicBasename+'.'+TopicIn
        if (self.topic_in not in self.topics):
#            raise ValueError('TopicIn does not exist: {}'.format(self.topic_in))
            print('Warning: Consumer created the topic: {}'.format(self.topic_in))
            new_topic = admin.NewTopic(self.topic_in, topicParams[0],topicParams[1])
            self.kafka_admin.create_topics([new_topic,])
            
        #print('TopicIn exists: {}'.format(TopicIn))
        params=self.connectParams
        params['group.id']=consumerID  # change this if you want to restart from the first/newest message
        params['default.topic.config']=topicConfig
           
        consumer=Consumer(params) # producer only used for log
        consumer.subscribe([self.topic_in])
        consumer.poll(0.1)
        return consumer
    
    def setLogChannel(self,LogTopic,topicParams=[1,1]):
        self.producer=self.makeProducer(LogTopic,topicParams)
        
    def setCommandChannel(self,CommandTopic,ConsumerID,topicConfig={'auto.offset.reset':'latest'}):
        self.commander=self.makeConsumer(CommandTopic,ConsumerID,topicConfig)
        
    def setConfigChannel(self,ConfigTopic,ConsumerID,topicConfig={'auto.offset.reset':'earliest'}):
        self.config=self.makeConsumer(ConfigTopic,ConsumerID+'{:04d}'.format(random.randint(0,9999)),topicConfig) 
            # make the consumer group random so as to ensure reading a pre-existing configuration and applying only the latest one..
        
            
    def setDevice(self,device,devicename):
        self.device=device
        self.deviceName=devicename
        return self.device.test()
    
    def logResult(self,keyword=''):
        keyW=keyword
        if (keyW==''):
            keyW=self.deviceName
            
        if (self.device):
            try:
                res=self.device.getInfo()
                if (len(res.keys())!=0):  # only log if data is available
                    self.producer.produce(self.topic_out,key=keyW,value=json.dumps(res))
                else:
                    print('Nothing in the collections')
            except:
                print('Problem getting info!')
            
        else:
            print('Set a device first')
            
    # returns an event value or nothing
    # parameter is the consumer to use (config or command)
    def doSinglePoll(self,consumer):
        mm=consumer.poll(0.1)   # MODIFY use command or config consumer!
        if (mm):
            if (mm.error()):
                errName=mm.error().name()
                if (errName!='_PARTITION_EOF'):
                    print('There was a problem with the consumption: {}'.format())
                    return None
                else:
                    return None# just return an empty list as no real message is received!
            else:
                #print('Received: {} with offset {} from topic {}'.format(mm.value(),mm.offset(),mm.topic()))
                #res=self.elk.index(mm.topic().lower(),'_doc',body=mm.value())
                try:
                    js=json.loads(mm.value())   # in most cases the message will be a json!
                except:
                    js=mm.value()
                return js
#                print(RR)
        else:
            return None

    # this has an internal loop to catch up untill the last available message
    # => will pass on a list of events to the device
    def doFullPoll(self,consumer):
#        print('Starting loop')
        finished=False
        eventList=[]
        while (not finished):
            RR=self.doSinglePoll(consumer)
            if (RR):
                eventList.append(RR)
            else:
                finished=True
                
        return eventList
    
    def checkCommands(self):
        eventList=self.doFullPoll(self.commander)
        if (len(eventList)>0):
            if (self.device):
                # in principle will send everything, device to decide if it will look at all the events
                self.device.doCommands(eventList) 
                print('Will send command to device: {}'.format(eventList[-1]))
            else:
                print('Amount of events gathered: {}'.format(len(eventList)))
                print('Consumed but no device: {}'.format(eventList[-1]))


    def checkConfig(self):
        eventList=self.doFullPoll(self.config)
        if (len(eventList)>0):
            if (self.device):
                # in principle will send everything, device to decide if it will look at all the events
                self.device.doConfig(eventList) 
                print('Will send config to device: {}'.format(eventList[-1]))
            else:
                print('Amount of events gathered: {}'.format(len(eventList)))
                print('Consumed but no device: {}'.format(eventList[-1]))
        
        
        

