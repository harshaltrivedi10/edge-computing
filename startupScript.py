import boto3
from boto3 import Session
import time
import subprocess
import threading
import urllib
import re
import random

sqs_video = "https://sqs.us-east-1.amazonaws.com/655728547640/video-queue.fifo"
sqs_input = "https://sqs.us-east-1.amazonaws.com/655728547640/input-queue.fifo"
accessKeyId = "AKIAZRLDXS44MZAGBHVN"
secretAccessKey = "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"

session = Session(aws_access_key_id = accessKeyId, aws_secret_access_key = secretAccessKey)
sqs = session.client('sqs', region_name='us-east-1')
s3 = session.resource('s3', region_name='us-east-1')
s3_client = session.resource('s3', region_name='us-east-1')
ec2 = session.resource('ec2', region_name='us-east-1')
outputBucket = s3.Bucket('cse546-output-bucket')


def stopInstance():
    instanceid = [str(urllib.request.urlopen('http://169.254.169.254/latest/meta-data/instance-id').read().decode())]
    ec2.instances.filter(InstanceIds = instanceid).stop()

def uploadOutput(videoName, output):
    print("Generating results and uploading it in S3")
    fileName = videoName.split(".")[0] + ".h264"
    output = output.decode(encoding = "utf-8")

    pattern = re.compile("[A-Za-z]+[:]\s[0-9]+\%")
    objects = set()

    f = re.findall(pattern, output)
    for i in f:
        objects.add(i.split(":")[0])
    if len(objects) == 0:
        objects.add("No Object Detected")
    with open(fileName, 'w') as f:
            f.write(",".join([o for o in objects]))

    outputBucket.upload_file(fileName, fileName)
    print("Upload Complete")

def runDarknet(videoName):
    print("Running Darknet")
    darknetCommand = ['/home/ubuntu/darknet/darknet', 'detector', 'demo', '/home/ubuntu/darknet/cfg/coco.data', '/home/ubuntu/darknet/cfg/yolov3-tiny.cfg', '/home/ubuntu/darknet/yolov3-tiny.weights',"/home/ubuntu/darknet/"+videoName]
    output = subprocess.run(darknetCommand, stdout = subprocess.PIPE).stdout
    uploadOutput(videoName, output)
    print("Completed Running Darknet")

def videoProcessor(videoName):
    print("In VideoProcessor")
    print(videoName)
    session.client('s3').download_file('cse546-video-bucket', videoName, videoName)
    print("Video Downloaded.....")
    runDarknet(videoName)
def main():
    newRequest = sqs.receive_message(QueueUrl=sqs_video, MessageAttributeNames=['All', ], MaxNumberOfMessages=1, VisibilityTimeout=330, WaitTimeSeconds=10)
    if "Messages" in newRequest:
        videoName = newRequest['Messages'][0]['Body']
        videoProcessor(videoName)
        print("Deleting SQS Message")
        sqs.delete_message(QueueUrl=sqs_video, ReceiptHandle=newRequest['Messages'][0]['ReceiptHandle'])
        print("Message Deleted")
        main()
    else:
        try:
            print("--------------------------------------------------In ELse ------------------------------------------------------------------")
            msg = sqs.receive_message(QueueUrl=sqs_input, MessageAttributeNames=['All', ], MaxNumberOfMessages=1, WaitTimeSeconds=1)
            videoName = msg['Messages'][0]['Body']
            print(videoName)
            sqs.delete_message(QueueUrl=sqs_input, ReceiptHandle=msg['Messages'][0]['ReceiptHandle'])
            sqs.send_message(QueueUrl = sqs_video, MessageBody = videoName, MessageGroupId = str(random.randint(0, 100000000000000)))
            print("-----------------------------------------------------Done Video Transfer :- "+videoName+"------------------------------------------------------------------------")
            main() 
        except:
            print("Stopping Instance...")
            stopInstance()

if __name__ == "__main__":
    main()
