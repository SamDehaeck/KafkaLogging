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
#import asyncio

class KafkaInOut():
    def __init__(self,TopicOut,TopicIn='none',ConsumerID='ConsumerId',connectParams=None,connectParamsFile='Vinnig/KafkaConnectionSettings',
                 topicParams=[1,1],topicBasename=''):
        if (connectParams==None):  # then read the configuration file. Location is relative to home directory
            home = os.path.expanduser("~")
            filename=os.path.join(home,connectParamsFile)
            with open(filename) as f:
                js=json.load(f)
                connectParams=js['Kafka']['connectParams']
                topicBasename=js['Kafka']['topicBasename']
        
        self.kafka_admin = admin.AdminClient(connectParams)
        self.topics=self.kafka_admin.list_topics().topics
        
        # setup producer
        self.topic_out=TopicOut
        
        if (TopicOut in self.topics):
            print('TopicOut exists: {}'.format(TopicOut))
        else:
            print('Should create TopicLog')
            new_topic = admin.NewTopic(TopicOut, topicParams[0],topicParams[1])
            self.kafka_admin.create_topics([new_topic,])
            
        self.producer=Producer(connectParams) # producer only used for log
        
        # Set up consumer
        self.topic_in=TopicIn
        if (TopicIn=='none'):
            print('Not using consumer!')
        else:
            if (TopicIn not in self.topics):
                raise ValueError('TopicIn does not exist')
                
            print('TopicIn exists: {}'.format(TopicIn))
            params=connectParams
            params['group.id']=ConsumerID  # change this if you want to restart from the first/newest message
            params['default.topic.config']={'auto.offset.reset':'earliest'}
               
            self.consumer=Consumer(params) # producer only used for log
            self.consumer.subscribe([TopicIn])
            self.consumer.poll(0.1)
        
        
        #self.consumer=Consumer(connectParams) # consumer only for commands?
        self.device=None
        self.deviceName='Unknown'
            
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
    def consumeSingleEvent(self):
        mm=self.consumer.poll(0.1)
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
    def consumeEvents(self):
#        print('Starting loop')
        finished=False
        eventList=[]
        while (not finished):
            RR=self.consumeSingleEvent()
            if (RR):
                eventList.append(RR)
            else:
                finished=True
        if (len(eventList)>0):
            if (self.device):
                # in principle will send everything, device to decide if will look at everything
                self.device.setInfo(eventList)
#                print('Will send something to device: {}'.format(eventList[-1]))
            else:
                print('Amount of events gathered: {}'.format(len(eventList)))
                print('Consumed but no device: {}'.format(eventList[-1]))

#        print('-----------')
        
        
        

