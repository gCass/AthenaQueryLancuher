import boto3
import configparser
config = configparser.ConfigParser()
config.read('conf/aws_data.ini')
# print(config.sections())

access_key = config["DEFAULT"]["aws_access_key_id"]
secret_key =config["DEFAULT"]["aws_secret_access_key"]
token = config["DEFAULT"]["aws_session_token"]

client = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    aws_session_token=token
)

# for bucket in client.list_buckets():
#     print(bucket)


