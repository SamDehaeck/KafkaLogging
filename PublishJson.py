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

class Publisher():
    def __init__(self):
        self.producer=None
        self.connectParams=None
        self.kafka_admin=None
        self.topics=None
        
    def setConnection(self,connectParams={'bootstrap.servers': 'localhost:9092'}):
        self.connectParams=connectParams
        self.producer=Producer(connectParams) # producer only used for log
        self.producer.poll(0.1)
        self.kafka_admin = admin.AdminClient(connectParams)
        self.topics=self.kafka_admin.list_topics().topics
        
        
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
            
    def readAndPublish(self,filename):
        #print('Going to open: {}'.format(filename))
        js={}  # empty dict for in case reading fails
        with open(filename) as f:
            js=json.load(f)
        try:
            if (self.producer==None):
                self.setConnection(js['Kafka']['connectParams'])
            if (js['Kafka']['topic'] not in self.topics):
                self.testTopic(js['Kafka']['topic'],js['Kafka']['topicParams'])
            self.publishJson(js['Kafka']['topic'],js['Json'])

        except:
            print('Some problem with json formatting probably: {}'.format(js['Json']))
            

# the code run if I run the program from the command line
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('jsonFile',help='Json file to upload')
    
    args=parser.parse_args()
    
    PP=Publisher()
    PP.readAndPublish(args.jsonFile)

        
        

