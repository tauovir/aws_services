import boto3
import os
from botocore.exceptions import ClientError

class Clients3:

    ACCESS_KESY = os.getenv('S3_ACCESS_KEY')
    SECRET_KEY = os.getenv('S3_SECRET_KEY')
    BUCKET_NAME = 'tauovir-tcs-bkt707'

    def __init__(self):

        self.s3_client = boto3.client("s3", aws_access_key_id=Clients3.ACCESS_KESY,
                                      aws_secret_access_key=Clients3.SECRET_KEY)

    def display_msg(self,status=1,msg = 'success',error = None):

        return {"status":status,"msg" : msg, "error": error}



    def create_bucket(self, bucket_name):

        try:
            response = self.s3_client.create_bucket(Bucket = bucket_name,ACL = 'public-read',CreateBucketConfiguration={'LocationConstraint':'ap-south-1'})
            return self.display_msg(msg="Bucket created successfully")

        except ClientError as e:
            return self.display_msg(status =2,msg = "error",error=str(e))


    def upload_object(self,file_name,bucket_name,key = None):

        if key is None:
            key = file_name.split('/')[-1]
        with open(file_name, 'rb') as data:
            self.s3_client.upload_fileobj(data, bucket_name, key)

    def delete_object(self,bucket_name,key):
        self.s3_client.delete_object(Bucket = bucket_name,Key = key)


    def get_objects(self):

        bucket_name = Clients3.BUCKET_NAME

        if not bucket_name:
            raise ValueError("Bucket Name required")
        all_objects = self.s3_client.list_objects(Bucket=bucket_name)

        return all_objects['Contents']

    def read_file(self, key,):

        bucket_name = Clients3.BUCKET_NAME
        data = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        contents = data['Body'].read().decode("utf-8").splitlines()
        return (len(contents))

    def row_count(self, s3_objects):

        dict_count = {}
        for s3_obect in s3_objects:
            if s3_obect['Size'] == 0:
                continue

            row_count = self.read_file(s3_obect['Key'])
            dict_count[s3_obect['Key']] = row_count
        return self.normalize(dict_count)

    def normalize(self, dict_count):

        result = {}

        for key, val in dict_count.items():
            key1 = key.split('/')[0]
            if key1 in result:
                result[key1] = result[key1] + val
            else:
                result[key1] = val

        return result


if __name__ == "__main__":
    obj = Clients3()
    # list_obj = obj.get_objects()
    # resp = obj.row_count(list_obj)
    # print(resp)
    # print(obj.create_bucket(bucket_name ="bina147863"))
    # obj.upload_object(file_name="/home/lenovo/Workforce/AMAZON/external_data/data/file1.txt",bucket_name='bina147863',key ="SALES/file1.txt")
    obj.delete_object(bucket_name='bina147863',key='SALES/housing.csv')

