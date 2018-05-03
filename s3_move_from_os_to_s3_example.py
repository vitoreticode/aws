import boto3
import botocore
import os
from os.path import isdir

os_path = "C:\\Users\\lucas.silva\\Documents\\arquivos-teste"

set_files = set()

for root, paths, files in os.walk(os_path):
    for name in files:
       set_files.add(os.path.join(root, name))

list_files = list(set_files)

s3_root = "emb-ikon-raw-dev"
bucket = "/".join(list_files[0].split("\\")[0:-1])
s3_bucket = bucket.replace("C:/Users/lucas.silva/Documents/arquivos-teste",s3_root).replace("-","_")
key = "/fhdb/arquivos-teste" + str(list_files[0].split("\\")[-1])


s3 = boto3.resource('s3')

s3.create_bucket(
    Bucket=s3_bucket, 
    CreateBucketConfiguration={
        'LocationConstraint': 'us-east-2'
    }
)

s3.Object(bucket, key).put(Body=open(list_files[0], 'rb'))
