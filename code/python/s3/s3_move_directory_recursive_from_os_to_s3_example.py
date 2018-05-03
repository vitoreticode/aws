# -*- coding: utf-8 -*-

"""Module to import a entire directory recursive on AWS S3
"""

import boto3
import botocore
import os
from os.path import isdir

# set the directory to be imported on S3
os_path = "C:\\Users\\username\\Documents\\files"


"""Function to read a directory and return a list with the absolute path of files

    Keyword arguments:

    path -- directory to be read
"""
def return_files(path):
    set_files = set()

    for root, _, files in os.walk(path):
        for name in files:
            set_files.add(os.path.join(root, name))

    list_files = list(set_files)

    return list_files


"""Function to save the files in S3

    Keyword arguments:

    files -- list of files with absolute path
"""
def put_s3_bucket(files):
    s3_root = "emb-ikon-raw-dev"

    for item in files:

        s3_path = "/".join(item.split("\\")[0:-1]) \
            .replace("C:\\Users\\username\\Documents\\files","bucket-prefix") \
            .replace("-","_") \
            .lower()

        s3_path_file = s3_path + "/{}".format(str(item.split("\\")[-1]))

        s3 = boto3.resource('s3')

        x = s3.Object(s3_root, s3_path_file).put(Body=open(item, 'rb'))
        
        # check if the HTTP code is valid
        if x['ResponseMetadata']['HTTPStatusCode'] != 200:
            print("Error: {}".format(x['ResponseMetadata']['HTTPStatusCode']) )
        else:
            print("File {} Imported on AWS S3 with Success!".format(s3_path_file))


def main(path):
    files = return_files(path)
    put_s3_bucket(files)

if __name__ == "__main__":
    main(os_path)

