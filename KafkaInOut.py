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

class KafkaInOut():
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
        
    def makeProducer(self,TopicOut,topicParams=[1,1]):
        # setup producer
        finTopicOut=self.topicBasename+'.'+TopicOut
        print('Producer Topic: {}'.format(finTopicOut))
        
        if not (finTopicOut in self.topics):
            print('Should create Topic: {}'.format(finTopicOut))
            new_topic = admin.NewTopic(finTopicOut, topicParams[0],topicParams[1])
            self.kafka_admin.create_topics([new_topic,])
            
        #return lambda : self.produceOutput(finTopicOut,Producer(self.connectParams))
        return finTopicOut,Producer(self.connectParams)
    
        
    def makeConsumer(self,TopicIn,consumerID,topicConfig={'auto.offset.reset':'earliest'},randomID=False,topicParams=[1,1]):
        # Set up consumer
        if (randomID):
            consumerID=consumerID+'{:04d}'.format(random.randint(0,9999))
        finTopicIn=self.topicBasename+'.'+TopicIn
        print('Consumer Topic: {}'.format(finTopicIn))
        if (finTopicIn not in self.topics):
#            raise ValueError('TopicIn does not exist: {}'.format(self.topic_in))
            print('Warning: Consumer created the topic: {}'.format(finTopicIn))
            new_topic = admin.NewTopic(finTopicIn, topicParams[0],topicParams[1])
            self.kafka_admin.create_topics([new_topic,])
            
        #print('TopicIn exists: {}'.format(TopicIn))
        params=self.connectParams
        params['group.id']=consumerID  # change this if you want to restart from the first/newest message
        params['default.topic.config']=topicConfig
           
        consumer=Consumer(params) # producer only used for log
        consumer.subscribe([finTopicIn])
        consumer.poll(0.1)
        
        #return lambda : self.consumeInput(finTopicIn,consumer)
        return (finTopicIn,consumer)
    
    def setDevice(self,device,devicename):
        self.device=device
        self.deviceName=devicename
        return self.device.test()
    
    def produceOutput(self,theTopic,theProducer):
        if (self.device):
            keyW=self.deviceName
            try:
                res=self.device.askOutput(theTopic)
                if (len(res.keys())!=0):  # only log if data is available
                    #print('even got here! {}'.format(res))
                    theProducer.produce(theTopic,key=keyW,value=json.dumps(res))
                else:
                    print('Nothing in the collections')
            except Exception as e:
                print('Problem getting info! {}'.format(e))
            
        else:
            print('Set a device first')
            
    def consumeInput(self,theTopic,theConsumer):
        eventList=self.doFullPoll(theConsumer)
        if (len(eventList)>0):
            if (self.device):
                # in principle will send everything, device to decide if it will look at all the events
                self.device.giveInput(theTopic,eventList) 
                #print('Will send command to device: {}'.format(eventList[-1]))
            else:
                print('Amount of events gathered: {}'.format(len(eventList)))
                print('Consumed but no device: {}'.format(eventList[-1]))
            
    # returns an event value or nothing
    # parameter is the consumer to use (config or command)
    def doSinglePoll(self,consumer):
        mm=consumer.poll(0)   # MODIFY use command or config consumer!
        if (mm):
            if (mm.error()):
                errName=mm.error().name()
                if (errName!='_PARTITION_EOF'):
                    print('There was a problem with the consumption: {}'.format())
                    return None
                else:
                    return None# just return an empty list as no real message is received!
            else:
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
    
        
        

