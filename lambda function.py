import json
import boto3
import os

def lambda_handler(event, context):

    # within our bucket of interest, get the file location and put into variable
    s3 = boto3.client('s3','us-east-1')
    file_object = event["Records"][0]
    filename = str(file_object['s3']['object']['key'])
    getfileobject = s3.get_object(Bucket = 's3fuelbucket'  , Key = filename)
    file_content = getfileobject["Body"].read().decode('utf-8')
     
    # perform s3 select query on the the values that are of interst to us
    KEY = filename
    response = s3.select_object_content(
        Bucket ='s3fuelbucket'  ,
        Key =KEY,
        ExpressionType = 'SQL',
        Expression = " select s._1,s._2,s._5,s._7 from s3object s where s._6 = 'YES' ",
        InputSerialization = {'CSV': {"FileHeaderInfo":"NONE"}},
        OutputSerialization = {'CSV':{ "RecordDelimiter" :"\n"}},
    )
    
    # create a list of records that we will append to as we dind rows that match our criteria    
    records = []
    for event in response['Payload']:
        if 'Records' in event:
            records.append(event['Records']['Payload'].decode('utf-8'))
            records.append("\n")
        elif 'Stats' in event:
            stats = event['Stats']['Details']
    
    # make the appended values look pretty instead of a jumbled mess it normally comes out as here
    records = '\n'.join(records) 
    print(records)            
    print(len(records))     
    
    # if the nubmer of records is greater than 0 we will invoke the sns request, that is tied to one of our subscriptions 
    if len(records) > 0:
        snsclient = boto3.client('sns')
        snsarn = 'arn:aws:sns:us-east-1:[redacted id]:NotifyMe'
        message = str(records)
        snsresponse = snsclient.publish(TopicArn=snsarn,Message=message,Subject='The following sites have their default alarms off')
