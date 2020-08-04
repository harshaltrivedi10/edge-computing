import boto3
import time
import copy
import random

WORKER_CAP = 10
id = "AKIAZRLDXS44MZAGBHVN"
secret = "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"
sqs_client = boto3.client("sqs", aws_access_key_id = id , aws_secret_access_key = secret, region_name = "us-east-1")
sqs_input = "https://sqs.us-east-1.amazonaws.com/655728547640/input-queue.fifo"
sqs_video = "https://sqs.us-east-1.amazonaws.com/655728547640/video-queue.fifo"

ec2_resource = boto3.resource("ec2", aws_access_key_id = id , aws_secret_access_key = secret,  region_name = "us-east-1")
ec2_client = boto3.client("ec2", aws_access_key_id = id , aws_secret_access_key = secret, region_name = "us-east-1")

def get_instances(state): 
    inst = []
    instances = ec2_resource.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': [str(state)]}])
    for instance in instances:
        inst.append(instance.id)
    return inst

def scale_out(c_nums, w_req):
    stopped_instances = get_instances("stopped")
   
    global waiting_msg 
    waiting_msg= w_req
    global current_nums
    current_nums = c_nums
    while waiting_msg > 0 and current_nums < WORKER_CAP and len(stopped_instances) > 0:
        print(current_nums, waiting_msg)

        msg = sqs_client.receive_message(QueueUrl=sqs_input, MessageAttributeNames=['All', ], MaxNumberOfMessages=1, WaitTimeSeconds=10)
        videoName = msg['Messages'][0]['Body']
        sqs_client.delete_message(QueueUrl=sqs_input, ReceiptHandle=msg['Messages'][0]['ReceiptHandle'])
        sqs_client.send_message(QueueUrl = sqs_video, MessageGroupId = str(random.randint(0, 100000000000000)),MessageBody=videoName)
        
        ec2_client.start_instances(InstanceIds=[str(stopped_instances.pop())])
        
        waiting_msg -= 1
        
        running_instances = get_instances("running")
        current_nums = len(running_instances)-1
        #time.sleep(10)

def balance_load():
    
    # while True:
    #    try:
    #        msg = sqs_client.receive_message(QueueUrl=sqs_video, MessageAttributeNames=['All', ], MaxNumberOfMessages=1, WaitTimeSeconds=10)
    #        videoName = msg['Messages'][0]['Body']
    #        sqs_client.delete_message(QueueUrl=sqs_video, ReceiptHandle=msg['Messages'][0]['ReceiptHandle'])
    #        response = sqs_client.send_message(QueueUrl = sqs_input, MessageBody = videoName, MessageGroupId = str(random.randint(0,100000000000000)))
    #        print(response)
    #    except:
    #        break
    waiting_requests = int(sqs_client.get_queue_attributes(QueueUrl=sqs_input, AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]["ApproximateNumberOfMessages"])

    print("-------"+str(waiting_requests)+"-----")	
    if waiting_requests > 0:
       
        worker_list = get_instances("running") 
        
        if len(worker_list)-1 < WORKER_CAP:
            scale_out(len(worker_list)-1,waiting_requests)
    else:
        time.sleep(0.5)
        balance_load()
while True:
    balance_load()
    time.sleep(1.5)
