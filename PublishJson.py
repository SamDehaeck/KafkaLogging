#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  6 15:30:43 2019

@author: sam
"""

from confluent_kafka import Producer
import confluent_kafka.admin as admin
import json
import numpy as np
import time
import argparse
import os.path

class Publisher():
    def __init__(self):
        self.producer=None
        self.connectParams=None
        self.kafka_admin=None
        self.topics=None
        
    def setConnection(self, connectParamsFile='Vinnig/KafkaConnectionSettings.json'):
        try:
            home = os.path.expanduser("~")
            filenameC=os.path.join(home,connectParamsFile)
            with open(filenameC) as fc:
                jsC=json.load(fc)
                self.connectParams=jsC['connectParams']
                self.topicBasename=jsC['topicBasename']
                
            self.producer=Producer(self.connectParams) # producer only used for log
            self.producer.poll(0.1)
            self.kafka_admin = admin.AdminClient(self.connectParams)
            self.topics=self.kafka_admin.list_topics().topics
            
        except Exception as e:
            print('Problem connecting to Kafka: {}'.format(e))
        
        
        
    def testTopic(self,Topic,topicParams=[1,1]):
        if (not self.connectParams):
            print('Should setConnection first!')
            return False
        
        self.topics=self.kafka_admin.list_topics().topics  # refresh topic list
        if (Topic in self.topics):
            #print('TopicLog exists: {}'.format(TopicLog))
            pass
        else:
            print('Should create TopicLog')
            new_topic = admin.NewTopic(Topic, topicParams[0],topicParams[1])
            self.kafka_admin.create_topics([new_topic,])
        return True
        
    # will block until the message is acknowledged
    def publishJson(self,Topic,record):
        #res=self.device.getInfo()
        if (self.producer!=None):
            nu=int(np.round(time.time()*1000))
            record['@timestamp']=nu  # append timestamp for the upload
            #print('I will publish to: {} this message: {}'.format(Topic,record))
            self.producer.produce(Topic,key='PublishJson.Publisher',value=json.dumps(record))
            evs=0
            while(evs==0):
                evs=self.producer.poll(0.1) # Should keep the producer alive untill the message is send!!!
        else:
            print('Should setConnection first!!')
            
    def testJson(self,filename):
        js={}  # empty dict for in case reading fails
        try:
            with open(filename) as f:
                js=json.load(f)
            dummy=js['kafka']['topic']
            dummy=js['json']
            if (dummy!=None):
                return True
        except:
            return False
        return False
        
            
    def readAndPublish(self,filename):
        if (not self.connectParams):
            print('Should setConnection first!')
            return False

        js={}  # empty dict for in case reading fails
        with open(filename) as f:
            js=json.load(f)
        try:
            topic=self.topicBasename+'.'+js['kafka']['topic']
            if (topic not in self.topics):
                self.testTopic(topic,js['kafka']['topicParams'])
            print('Will publish to topic {}'.format(topic))
            self.publishJson(topic,js['json'])

        except Exception as e:
            print('Some problem with json formatting probably: {}'.format(e))
            

# the code run if I run the program from the command line
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('jsonFile',help='Json file to upload')
    
    args=parser.parse_args()
    
    PP=Publisher()
    PP.setConnection('Vinnig/KafkaConnectionSettings.json')
    PP.readAndPublish(args.jsonFile)

        
        

