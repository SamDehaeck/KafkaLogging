#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 11 17:46:26 2019

@author: sam
"""
import sys
from PyQt5.QtWidgets import QApplication, QMainWindow,QGridLayout,QWidget,QPushButton,QSizePolicy
#from PyQt5.QtCore import Qt
import glob
import os
import numpy as np
import PublishJson
import PublishDirectory

# Subclass QMainWindow to customise your application's main window
class MainWindow(QMainWindow):

    def __init__(self, *args, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)

        self.setWindowTitle("Vinnig Control Centre")

        layout = QGridLayout()
        
        home = os.path.expanduser("~")
        self.dirName=os.path.join(home,'Vinnig','json')
        self.connectKafka=os.path.join('Vinnig','KafkaConnectionSettings.json')
        
        self.publisher=PublishJson.Publisher()
        self.publisher.setConnection(self.connectKafka)
        
        self.timedPublisher=PublishDirectory.TimedPublisher(self.connectKafka)
        
        bList=self.getButtonList(self.dirName)
        bcount=len(bList)
        square=int(np.floor(np.sqrt(bcount)))
        
        for i in range(bcount):
            butt=bList[i]
            butt.clicked.connect(self.onButtonPressed)
            layout.addWidget(butt,i//square,i%square)

        widget = QWidget()
        widget.setLayout(layout)
        self.setCentralWidget(widget)

        
    ### Should look at a directory, find all valid json's and make into a pushbutton list (or a string list)
    def getButtonList(self,dirName):
        bList=[]
        
        root,dirs,files = next(os.walk(dirName)) # only need first level directories
        if (len(dirs)!=0):
            for dd in dirs:
                #newD=os.path.join(root,dd)
                butt=QPushButton(dd)
                butt.setSizePolicy(QSizePolicy.Expanding,QSizePolicy.Expanding)
                butt.setStyleSheet("background-color: red")
                bList.append(butt)
            
        filenames=glob.glob(os.path.join(os.path.abspath(dirName),"*.json"))
        
        for ff in filenames:
            if (self.publisher.testJson(ff)):
                butt=QPushButton(os.path.basename(ff))  # better to check if valid json and publisheable before adding to list!
                butt.setSizePolicy(QSizePolicy.Expanding,QSizePolicy.Expanding)
                bList.append(butt)
#            else:
#                print('Problem with {}'.format(ff))
        return bList
    
    ### will publish the json described in the filename on the button
    def onButtonPressed(self):
        fileName=self.sender().text()
        fullPath=os.path.join(self.dirName,fileName)
        if (os.path.isfile(fullPath)):
            self.publisher.readAndPublish(fullPath)
        else: # is directory
            self.timedPublisher.publishFolder(fullPath)
        
        

if __name__ == '__main__':
    app = QApplication(sys.argv)
    
    window = MainWindow()
    window.show()
    
    app.exec_()
