import boto3
import configparser
config = configparser.ConfigParser()
conf = config.read('conf/aws_data.txt')
access_key = conf["DEFAULT"]["aws_access_key_id"]
secret_key = conf["DEFAULT"]["aws_secret_access_key"]
token = conf["DEFAULT"]["aws_session_token"]

import boto3

client = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    aws_session_token=token
)

for bucket in client.buckets.all():
    print(bucket.name)

