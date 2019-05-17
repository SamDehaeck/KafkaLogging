#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  6 15:30:43 2019

@author: sam
"""

from confluent_kafka import Producer
import confluent_kafka.admin as admin
import json
#import asyncio

class KafkaLogger():
    def __init__(self,TopicLog,connectParams={'bootstrap.servers': 'localhost:9092'},
                 topicParams=[1,1]):
        self.kafka_admin = admin.AdminClient(connectParams)
        self.topics=self.kafka_admin.list_topics().topics
        self.topic_log=TopicLog
        
        if (TopicLog in self.topics):
            print('TopicLog exists: {}'.format(TopicLog))
        else:
            print('Should create TopicLog')
            new_topic = admin.NewTopic(TopicLog, topicParams[0],topicParams[1])
            self.kafka_admin.create_topics([new_topic,])
            
        self.producer=Producer(connectParams) # producer only used for log
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
            res=self.device.getInfo()
            if (len(res.keys())!=0):  # only log if data is available
                self.producer.produce(self.topic_log,key=keyW,value=json.dumps(res))
            else:
                print('Nothing in the collections')
            
        else:
            print('Set a device first')
        
        
        

