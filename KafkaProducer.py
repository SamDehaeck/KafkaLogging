#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  6 15:30:43 2019

@author: sam
"""

from confluent_kafka import Producer
import confluent_kafka.admin as admin
import json
import collections
import numpy as np
import time

class KafkaProducer():
    def __init__(self,TopicLog,connectParams={'bootstrap.servers': 'localhost:9092'},
                 topicParams=[1,1]):
        self.kafka_admin = admin.AdminClient(connectParams)
        self.topics=self.kafka_admin.list_topics().topics
        self.topic_log=TopicLog
        
        if (TopicLog in self.topics):
            #print('TopicLog exists: {}'.format(TopicLog))
            pass
        else:
            print('Should create TopicLog')
            new_topic = admin.NewTopic(TopicLog, topicParams[0],topicParams[1])
            self.kafka_admin.create_topics([new_topic,])
            
        self.producer=Producer(connectParams) # producer only used for log
            
    def publishJson(self,record):
        #res=self.device.getInfo()
        self.producer.produce(self.topic_log,key='myProducer',value=json.dumps(record))

    def makeRecordingJson(self,subcommand="No Subcommand"):
        systInfo=collections.OrderedDict()
        systInfo['@timestamp']=int(np.round(time.time()*1000))
        systInfo['target']='You'
        systInfo['command']="recording"
        
        subInfo=collections.OrderedDict()
        subInfo['subcommand']=subcommand
        subInfo['dirname']='/home/sam'
        subInfo['codec']='FMF'
        subInfo['recskip']=0

        systInfo['command_params']=subInfo
        
        self.publishJson(systInfo)
        
    def makeParameterJson(self,subcommand="No Subcommand"):
        systInfo=collections.OrderedDict()
        systInfo['@timestamp']=int(np.round(time.time()*1000))
        systInfo['target']='You'
        systInfo['command']='parameter'
        
        subInfo=collections.OrderedDict()
        subInfo['subcommand']=subcommand
        subInfo['shutter']=100
        subInfo['delaytime']=3.24
        subInfo['roirows']=1000
        subInfo['roicols']=1000

        systInfo['command_params']=subInfo
        
        self.publishJson(systInfo)

        
        

