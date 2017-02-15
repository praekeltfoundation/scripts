#!/usr/bin/env python

import argparse
import json
import requests
import sys


def create_sub(url, token, data):
    headers = {
        'Authorization': "Token " + token,
        'Content-Type': "application/json"
    }
    return requests.post('%ssubscriptions/' % url, headers=headers, json=data)


def get_messageset_schedule(url, token, messageset_id):
    headers = {
        'Authorization': "Token " + token,
        'Content-Type': "application/json"
    }
    messageset = requests.get('%smessageset/%s' % (url, messageset_id),
                              headers=headers).json()
    return messageset['default_schedule']


parser = argparse.ArgumentParser(description='Subscribe users to a specific '
                                 'message set.')
parser.add_argument('--sbm-url', required=True,
                    help='The url for the Stage Based Messaging service.')
parser.add_argument('--sbm-token', required=True,
                    help='The token for the Stage Based Messaging service.')
parser.add_argument('--messageset-id', type=int, required=True,
                    help='The id of the messageset to subscribe users to.')
parser.add_argument('--file', dest='data_file', type=argparse.FileType('r'),
                    help='Name of file containing the list of identities.')
parser.add_argument('--data', help='List of identities. One per line.')

args = parser.parse_args()
messageset_id = args.messageset_id
sbm_url = args.sbm_url
sbm_token = args.sbm_token

if args.data_file:
    identity_list = args.data_file.readlines()
elif args.data:
    identity_list = args.data.split("\n")

messageset_schedule = get_messageset_schedule(sbm_url, sbm_token,
                                              messageset_id)
for item in identity_list:
    identity = json.loads(item)
    data = {
        'identity': identity['identity'],
        'lang': identity['language'],
        'next_sequence_number': 1,
        'messageset': messageset_id,
        'schedule': messageset_schedule
    }
    response = create_sub(args.sbm_url, args.sbm_token, data)
    if response.status_code != 200 and response.status_code != 201:
        sys.stdout.write("Subscription creation failed - Identity: %s Error "
                         "code: %s\n" % (identity['identity'],
                                         response.status_code))
sys.stdout.write("Operation complete\n")
