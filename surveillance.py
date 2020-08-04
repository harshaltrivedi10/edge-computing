#!/usr/bin/python
'''
SETUP:

    -   -->     GND     -->     PIN6
    +   -->     5V      -->     PIN4
    S   -->     GPIO18  -->     PIN12

'''

import RPi.GPIO as GPIO
import subprocess
import time
import sys
import boto3
from boto3 import Session
from datetime import datetime
from picamera import PiCamera
import threading
import time
import re
import random
from subprocess import PIPE, run

accessKeyId = "AKIAZRLDXS44MZAGBHVN"
secretAccessKey = "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"

session = Session(aws_access_key_id = accessKeyId, aws_secret_access_key = secretAccessKey)
s3 = session.resource('s3')
s3_object = s3.Bucket('cse546-video-bucket')
s3OutputObject = s3.Bucket("cse546-output-bucket")
queue_url = "https://sqs.us-east-1.amazonaws.com/655728547640/input-queue.fifo"
sqs = boto3.client("sqs", region_name = "us-east-1", aws_access_key_id = accessKeyId, aws_secret_access_key = secretAccessKey)


def takeFromVideoQueue():
    waiting_requests = int(sqs.get_queue_attributes(QueueUrl="https://sqs.us-east-1.amazonaws.com/655728547640/vidoe-queue.fifo", AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]["ApproximateNumberOfMessages"])
    if waiting_requests > 0:
        msg = sqs.receive_message(QueueUrl=queue_url, MessageAttributeNames=['All', ], MaxNumberOfMessages=1, WaitTimeSeconds=10, VisibilityTimeout = 160)
        videoName = msg['Messages'][0]['Body']
        videoName = videoName[:-5]
        piThread = threading.Thread(target = processingOnPi, name = "piThread", args = (videoName, 1, msg['Messages'][0]['ReceiptHandle'] ))
        piThread.start()

def processingOnPi(videoname, num, receiptHandle):
    #videoname = videoname.split(".")[0]
    s3_object.upload_file(Filename = videoname+".h264", Key = videoname+".h264")
    darknetCommand = ['./darknet', 'detector', 'demo', 'cfg/coco.data', 'cfg/yolov3-tiny.cfg', 'yolov3-tiny.weights', videoname+".h264"]
    output = run(darknetCommand, stdout=subprocess.PIPE).stdout
    fileName = videoname +'.h264'
    
    #with open(fileName, 'w') as f:
        #f.write(str(output))

    pattern = re.compile("[A-Za-z]+[:]\s[0-9]+\%")
    objects = set()
    
    output = output.decode(encoding="utf-8")
    f = re.findall(pattern, output)

    for i in f:    
        objects.add(i.split(":")[0])

    if len(objects) == 0:
        objects.add("No Object Detected")
    with open(fileName, 'w') as fle:
        fle.write(", ".join([o for o in objects]))
        
    #s3 upload
    #s3_object.upload_file(Filename = videoname+".h264", Key = videoname+".h264")
    s3OutputObject.upload_file(Filename = videoname+".h264", Key =  videoname+".h264")
    
    if num == 1:
        print("This was from sqs")
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receiptHandle)

def processingOnAws(videoname):
    cnt = random.randint(0,1000000000000000000)
    sqs.send_message(QueueUrl = queue_url, MessageBody=videoname+".h264", MessageGroupId="CCVideo"+str(cnt))
    s3_object.upload_file(Filename = videoname+".h264", Key = videoname+".h264")
    #sqs.send_message(QueueUrl = queue_url, MessageBody=videoname+".h264", MessageGroupId="CCVideo"+str(cnt))
    

count = 0
aliveThread = False 

while (True):
    
    global piThread 
    #piThread = threading.Thread(target = processingOnPi, name = "piThread", args = (videoName, ))
    
    sensor = 12
    GPIO.setwarnings(False)
    GPIO.setmode(GPIO.BOARD)
    GPIO.setup(sensor, GPIO.IN)

    on = 0
    off = 0
    flag = 0
    
    while flag == 0:
        i=GPIO.input(sensor)
        if i == 0:
            if flag == 1:
                off = time.time()
                diff = off - on
                print ('time: ' + str(diff%60) + ' sec')
                print ('')
                flag = 0
		
            print ("No intruders")
            threads =  [thread.name for thread in threading.enumerate()]
	    
            if "piThread" not in threads:
                waiting_requests = int(sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]["ApproximateNumberOfMessages"])
                if waiting_requests > 0:
                    msg = sqs.receive_message(QueueUrl=queue_url, MessageAttributeNames=['All', ], MaxNumberOfMessages=1, WaitTimeSeconds=10, VisibilityTimeout = 160)
                    videoName = msg['Messages'][0]['Body']
                    videoName = videoName[:-5]
                    piThread = threading.Thread(target = processingOnPi, name = "piThread", args = (videoName, 1, msg['Messages'][0]['ReceiptHandle'] ))
                    piThread.start()
                #else:
                   # takeFromVideoQueue()
            time.sleep(1)
        elif i == 1:
            if flag == 0  and count < 10:
                print ("Intruder detected")
                on = time.time()
                flag = 1
        
                videoName = str(datetime.today()).split(".")[0]
                #recordVideo(videoName)
                camera = PiCamera()
                camera.resolution = (800, 600)
                camera.start_recording(videoName+".h264")
                camera.wait_recording(5)
                camera.stop_recording()
                camera.close()

                awsThread = threading.Thread(target = processingOnAws, name = "awsThread", args = (videoName,))
		
                threads =  [thread.name for thread in threading.enumerate()]
                
                if "piThread" in threads:
                    print("In AWS")
                    awsThread.start()
                else:
                    piThread = threading.Thread(target = processingOnPi, name = "piThread", args = (videoName, 0, "", ))
                    piThread.start()

                time.sleep(0.1)
    count += 1
    print(count)
