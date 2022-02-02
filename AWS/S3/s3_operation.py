
import boto3
from botocore.exceptions import ClientError
import os
import logging
from AWS.common.helper import Test


ACCESS_KESY = os.getenv('S3_ACCESS_KEY')
SECRET_KEY = os.getenv('S3_SECRET_KEY')
PRI_BUCKET_NAME = "filmcli01"
TRANSIENT_BUCKET_NAME = "filmcli02"

F1 = "file1.txt"
F2 = "file1.txt"
F3 = "file1.txt"
DIR = "/home/lenovo/Workforce/AMAZON/external_data/data/"
DOWN_DIR = "/home/lenovo/Workforce/AMAZON/external_data/download_data"



def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True



def read_s3_object(s3,bucket,key):

    obj = s3.Object(bucket, key)
    # resp = obj.get()['Body'].read().decode('utf-8')
    client = boto3.client("s3",aws_access_key_id = ACCESS_KESY,aws_secret_access_key = SECRET_KEY)
    all_objects = client.list_objects(Bucket='tauovir-tcs-bkt707')
    print("==============")
    print(all_objects)
    # bucket = s3.Bucket('tauovir-tcs-bkt707')
    # print(bucket)
    print(30*"=")




def create_bucket(s3,name,region='ap-south-1'):
    location = {'LocationConstraint': region}
    try:
        bucket = s3.create_bucket(Bucket = name, CreateBucketConfiguration = location)

    except Exception as e:
        print(e)



if __name__== '__main__':
    import os
    print("Started========")
    s3 = boto3.resource('s3',aws_access_key_id = ACCESS_KESY,aws_secret_access_key = SECRET_KEY)

    # create_bucket(s3,PRI_BUCKET_NAME)os.path.basename(file_name)
    # upload_file(PRI_BUCKET_NAME, DIR, F1, s3)
    #upload_file(DIR +F2,PRI_BUCKET_NAME,'khan')
    # read_s3_object(s3,PRI_BUCKET_NAME,'sales')
    print(os.getenv('S3_ACCESS_KEY'))

