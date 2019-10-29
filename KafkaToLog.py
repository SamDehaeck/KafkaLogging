#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  6 15:30:43 2019

@author: sam
"""

from confluent_kafka import Consumer#,TopicPartition#,OFFSET_BEGINNING
#import random
import signal
import os.path
import json
#import asyncio

class KafkaToLog():
    def __init__(self,groupid,writeFolder,connectParamsFile='Vinnig/KafkaConnectionSettings.json'):
        home = os.path.expanduser("~")
        filename=os.path.join(home,connectParamsFile)
        with open(filename) as f:
            js=json.load(f)
            params=js['connectParams']
            params['group.id']=groupid  # change this if you want to restart from the first message
            params['default.topic.config']={'auto.offset.reset':'earliest'} # earliest is important; starts from beginning if groupid is a new one for this topic
            
        self.topicRegex='^'+js['topicBasename']+'.*'
        self.writeFolder=writeFolder
        self.currentTopics=[] # an empty list for now
        self.consumer=Consumer(params)
        self.consumer.subscribe([self.topicRegex],on_assign=self.newTopicAssigned)
        self.consumer.poll(0.1)

            
    def newTopicAssigned(self,consumer,listPartitions):
        #print('Found {} topics'.format(len(listPartitions)))
        for p in listPartitions:
            if not(p in self.currentTopics): # new topic is detected!!
                print('Found topic {}'.format(p.topic))
                self.currentTopics.append(p)
                #print(p)

        
    def relogResult(self,timeout):
        #print('Doing a poll')
        mm=self.consumer.poll(timeout)
        if (len(self.currentTopics)==0):
            #print('Problem with assignment of topics')
            return True

        if (mm):
            if (mm.error()):
                errName=mm.error().name()
                if (errName!='_PARTITION_EOF'):
                    print('There was a problem with the consumption: {}  for {}'.format(errName,mm.topic()))
                    return False
                else:
                    return True
            else:
                #print('Received: {} with offset {} from topic {}'.format(mm.value(),mm.offset(),mm.topic()))
                top=os.path.join(self.writeFolder,mm.topic().lower().replace('.','_')+'.csv')
                outString,keystring=self.messageToString(mm.value())
                #print(outString)
                writeHeader= not os.path.exists(top)
                
                with open(top,mode='a') as F:
                    if (writeHeader):
                       F.writelines(keystring+'\n') 
                    F.writelines(outString+'\n')
                return True
                # probably should check for errors for res
        else:
            print('No message received')
            return False
        
    def messageToString(self,message):
        T=json.loads(message)
        St=''
        keySt=''
        allKeys=T.keys()
        sortKeys=sorted(allKeys)     # sort alphabetically to ensure same ordering each time
        for kk in sortKeys:
            St=St+'{}\t'.format(T[kk])
            keySt=keySt+'{}\t'.format(kk)
        # if necessary add @timestamp to have timestamp always in first place..
        return St,keySt
            
    def close(self):
        self.consumer.close()
      
        
# the code run if I run the program from the command line
if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.default_int_handler)

    logpath = os.path.join(os.path.expanduser("~"),'Vinnig','Logs')
#    group_id='kafkaToLog{:04d}'.format(random.randint(0,9999))
    group_id='kafkaToLog'
    kk=KafkaToLog(group_id,logpath)
    
    try:
        worked=True
        while worked:
            worked=kk.relogResult(5)
        print("I think I'm done!!!")
    except KeyboardInterrupt:
        print('Pressed Ctrl-C')
    finally:
        kk.close()
            
        
        
        

