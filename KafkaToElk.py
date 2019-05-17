#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  6 15:30:43 2019

@author: sam
"""

from confluent_kafka import Consumer#,TopicPartition#,OFFSET_BEGINNING
import elasticsearch
import argparse
import signal

#import json
#import asyncio

class KafkaToElk():
    def __init__(self,KafkaTopic,groupid,kafkaConnectParams={'bootstrap.servers': 'localhost:9092'}):
        
        # now look into connection with ElasticSearch
        self.elk=elasticsearch.Elasticsearch() # not yet using special connection parameters
        self.mymapping={"mappings": { "date_detection": "false", "dynamic_templates": [ { "dates": { "match": "@timestamp|date", "match_pattern": "regex", "mapping": { "type": "date" } } } ] } } 
        
        params=kafkaConnectParams
        params['group.id']=groupid  # change this if you want to restart from the first message
        params['default.topic.config']={'auto.offset.reset':'earliest'} # earliest is important; starts from beginning if groupid is a new one for this topic
        self.currentTopics=[] # an empty list for now
        self.consumer=Consumer(params)
        self.consumer.subscribe([KafkaTopic],on_assign=self.newTopicAssigned)
        self.consumer.poll(0.1)

            
    def newTopicAssigned(self,consumer,listPartitions):
        print('Found {} topics'.format(len(listPartitions)))
        for p in listPartitions:
            if not(p in self.currentTopics): # new topic is detected!! => check if ELK index exists
                self.currentTopics.append(p)
                if not(self.elk.indices.exists(p.topic.lower())):
                    self.elk.indices.create(p.topic.lower(),body=self.mymapping)

        
    def relogResult(self,timeout):
        mm=self.consumer.poll(timeout)
        if (len(self.currentTopics)==0):
            #print('Problem with assignment of topics')
            pass
    

        if (mm):
            if (mm.error()):
                errName=mm.error().name()
                if (errName!='_PARTITION_EOF'):
                    print('There was a problem with the consumption: {}'.format())
            else:
                #print('Received: {} with offset {} from topic {}'.format(mm.value(),mm.offset(),mm.topic()))
                #res=self.elk.index(mm.topic().lower(),'_doc',body=mm.value())
                res=self.elk.index(mm.topic().lower(),body=mm.value())
                goodShards=res['_shards']['successful']
                if (goodShards==0):
                    print('Problem with elasticsearch indexing')
                # probably should check for errors for res
        else:
            #print('No message received')
            pass
            
    def close(self):
        self.consumer.close()
        

      
        
# the code run if I run the program from the command line
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('kafka_topic',help='Kafka topic or wildcard (e.g. ^*log)')
    parser.add_argument('-g','--group_id',help='Kafka group id', default='kafkatoelk')
    
    args=parser.parse_args()
    
    signal.signal(signal.SIGINT, signal.default_int_handler)

    kk=KafkaToElk(args.kafka_topic,args.group_id)
    
    try:
        while True:
            kk.relogResult(0.1)
    except KeyboardInterrupt:
        print('Pressed Ctrl-C')
    finally:
        kk.close()
            
        
        
        

