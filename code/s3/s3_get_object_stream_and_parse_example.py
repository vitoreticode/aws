# -*- coding: utf-8 -*-

import boto3
import json
from pprint import pprint as pp


"""This function save a list into a file

    Keyword arguments:

    list_to_be_converted -- simple list
    filename -- the name of the file to the list be saved
"""

def list_to_file(list_to_be_converted, filename):
    with open(filename,"w") as outfile:
        for line in list_to_be_converted:
            outfile.write(line)
            outfile.write("\n")

# create aws client
client = boto3.client('s3')

# define the path on aws s3 of the file
bucket="bucket-name"
key="key-name"

# male the request of the object
file = client.get_object(Bucket=bucket,Key=key)

# decode the requested streaming object to string with windows charset
res = json.loads(json.dumps(file['Body'].read().decode("cp1251")))

# break lines on "end of line" with windows charset
list_res = res.split("\r\n")

# call a function to save the list into a file
list_to_file(list_res,"test-decode-get-object.fhd")


