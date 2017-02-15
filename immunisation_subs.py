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


parser = argparse.ArgumentParser(description='Subscribe users to an '
                                 'immunisation message set.')
parser.add_argument('--sbm-url', required=True,
                    help='The url for the Stage Based Messaging service.')
parser.add_argument('--sbm-token', required=True,
                    help='The token for the Stage Based Messaging service.')
parser.add_argument('--messageset-ids', type=int, nargs=4, required=True,
                    help='The ids of the messagesets to subscribe users to. '
                    'They must be in the order users would receive the '
                    'messages.')
parser.add_argument('--file', dest='data_file', type=argparse.FileType('r'),
                    help='Name of file containing the list of identities.')
parser.add_argument('--data', help='List of identities. One per line.')

args = parser.parse_args()
sbm_url = args.sbm_url
sbm_token = args.sbm_token
messagesets = args.messageset_ids

if args.data_file:
    identity_list = args.data_file.readlines()
elif args.data:
    identity_list = args.data.split("\n")

for item in identity_list:
    messageset_id = None
    identity = json.loads(item)
    old_set = identity['current_messageset_id']
    new_set = identity['expected_messageset_id']
    old_msg = identity['current_sequence_number']
    new_msg = identity['expected_sequence_number']

    # fast-forwarding within set 8
    if old_set == 8 and new_set == 8:
        if old_msg <= 29 and new_msg > 29:
            messageset_id = messagesets[2]  # send 29
        elif old_msg <= 21 and new_msg > 21:
            messageset_id = messagesets[1]  # send 21
        elif old_msg <= 13 and new_msg > 13:
            messageset_id = messagesets[0]  # send 13
    # fast-forwarding within set 7
    elif old_set == 7 and new_set == 7:
        if old_msg <= 36 and new_msg > 36:
            messageset_id = messagesets[3]  # send 36

    # fast-forwarding to set 8
    elif old_set != 8 and new_set == 8:
        if new_msg > 29:
            messageset_id = messagesets[2]  # send 29
        elif new_msg > 21:
            messageset_id = messagesets[1]  # send 21
        elif new_msg > 13:
            messageset_id = messagesets[0]  # send 13

    # fast-forwarding to set 7
    elif old_set != 7 and new_set == 7:
        if new_msg > 36:
            messageset_id = messagesets[3]  # send 36
        # they didn't receive the last message of set 8
        elif old_set != 8 or old_msg <= 29:
            messageset_id = messagesets[2]  # send 29 of 8

    if messageset_id is not None:
        data = {
            'identity': identity['identity'],
            'lang': identity['language'],
            'next_sequence_number': 1,
            'messageset': messageset_id,
            'schedule': get_messageset_schedule(sbm_url, sbm_token,
                                                messageset_id)
        }
        response = create_sub(args.sbm_url, args.sbm_token, data)
        if response.status_code != 200 and response.status_code != 201:
            sys.stdout.write("Subscription creation failed - Identity: %s Error "
                             "code: %s\n" % (identity['identity'],
                                             response.status_code))
        else:
            sys.stdout.write("Subscription created - Identity: %s" %
                             identity['identity'])
    else:
        sys.stdout.write("No messages for Identity %s" % identity['identity'])
sys.stdout.write("Operation complete\n")
