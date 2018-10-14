# -*- coding: utf-8 -*-

#!/usr/bin/python
import os
import logging
import boto3
import botocore
import tempfile
import subprocess
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - line %(lineno)s - function %(funcName)s - '
                                  '%(message)s'))

logger.addHandler(sh)

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

tempdir = tempfile.gettempdir()

def remove_file(file_location):
    try:
        os.remove(file_location)
    except OSError:
        pass

def s3_download(bucket, key):
    file_name = key.split('/')[-1]
    file_loc = os.path.join(tempdir, file_name)
    bucket_path = "%s/%s" % (bucket, key)
    #remove_file(file_loc)

    try:
        file_meta = s3_client.get_object(Bucket=bucket, Key=key)['Metadata']
        s3.Bucket(bucket).download_file(key, file_loc)
        logger.info("File s3://" + bucket_path + " downloaded to " + file_loc)

        return {'file_location': file_loc, 'metadata': file_meta}
    except botocore.exceptions.ClientError as e:
        logger.info("Error on dowload from S3: " + str(e))

def s3_upload(file_path, bucket, key,):
    #key = key.lower()
    logger.info("Uploading file %s to %s." % (file_path, os.path.join(bucket, key)))
    try:
        s3_client.upload_file(file_path, bucket, key)
    except botocore.exceptions.ClientError as e:
        logger.info("Error on upload to S3: " + str(e))

    return 0, ""

def main(event, context):

    print("Event : " + str(event))
    print("Context : " + str(context))

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    logger.info('Downloading file on: {0}/{1}'.format(bucket,key))
    file_metadata = s3_download(bucket, key)
    file_location = file_metadata['file_location']

    logger.info('Starting cleasing file: {0}'.format(file_location))
    # Read in the file
    with open(file_location, 'r') as file :
        filedata = file.read()

        # Replace the target string
        filedata = filedata.replace('""', '"')
        filedata = filedata.replace('","', ',')
        filedata = filedata.replace('$data', 'data')
        filedata = filedata.replace('$iod', 'iod')

    # Write the file out again
    with open(file_location, 'w') as file:
        file.write(filedata)

    logger.info('Finish cleasing file: {0}'.format(file_location))

    upload_result = s3_upload(file_location,bucket, key)

    if upload_result[0] != 0:
        logger.info("Error on upload file {0} to {1}{2}".format("file_location",bucket, key))
    else:
        logger.info("Cleaned csv file uploaded from {0} to {1}/{2}.".format(file_location,bucket, key))
    
