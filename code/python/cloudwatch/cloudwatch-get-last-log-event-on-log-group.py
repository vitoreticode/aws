# -*- coding: utf-8 -*-

import base64
import boto3
import botocore
import json
from pprint import pprint as pp
import re
import sys
import time

# arg1= sys.argv[1]

session = boto3.session.Session(profile_name='xxxxxx')

cloudwatch_client = session.client('logs')

def get_last_log_streams(logGroupName):
    log_streams = cloudwatch_client.describe_log_streams(
        logGroupName=logGroupName,
        orderBy='LastEventTime',
        descending=True,
        limit=50
    )
    return log_streams['logStreams'][0]['arn'].split(':')[-1:]


def get_last_log_events(logGroupName, last_log_stream):
    log_events = cloudwatch_client.get_log_events(
        logGroupName=logGroupName,
        logStreamName=last_log_stream[0],
        startFromHead=True
    )
    return log_events['events'][-100:]


def print_messages(last_log_events):
    for item in last_log_events:
        print(item['message'])


def main(logGroupName):
    last_log_stream = get_last_log_streams(logGroupName)
    last_log_events = get_last_log_events(logGroupName, last_log_stream)
    print_messages(last_log_events)

# def __main__(arg1):
#     main(arg1)

if __name__ == "__main__":
    main(sys.argv[1])

# /aws/lambda/ikon_fhdb_fdecorrelations_dev
