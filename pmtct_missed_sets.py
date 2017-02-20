#!/usr/bin/env python

import argparse
import json
import requests
import sys
from collections import OrderedDict

set_details = OrderedDict()
set_details[3] = {'seq': [8, 15, 24]}
set_details[4] = {'seq': [2]}
set_details[5] = {'seq': [5]}
set_details[1] = {'seq': [6, 15, 17]}

# TODO: this should be updated to the correct ids in Prod
new_set_ids = {
    1: 24,
    2: 25,
    3: 26,
    4: 27,
    5: 28,
    6: 29,
    7: 30,
    8: 31,
}

# This is what the sets will look like in the data base, we choose the message
# set based on where we want to end and start at the first important message
# the subscription missed.

# 1: 8
# 2: 8, 15
# 3: 8, 15, 24
# 4: 8, 15, 24, 2
# 5: 8, 15, 24, 2, 5
# 6: 8, 15, 24, 2, 5, 6
# 7: 8, 15, 24, 2, 5, 6, 15
# 8: 8, 15, 24, 2, 5, 6, 15, 17

session = requests.Session()
http = requests.adapters.HTTPAdapter(max_retries=5)
https = requests.adapters.HTTPAdapter(max_retries=5)
session.mount('http://', http)
session.mount('https://', https)

schedules = {}


def create_sub(url, token, data):
    headers = {
        'Authorization': "Token " + token,
        'Content-Type': "application/json"
    }

    r = session.post('%ssubscriptions/' % url, data=json.dumps(data),
                     headers=headers)

    r.raise_for_status()
    return r


def get_messageset_schedule(url, token, messageset_id):
    if messageset_id not in schedules:
        headers = {
            'Authorization': "Token " + token,
            'Content-Type': "application/json"
        }
        r = session.get('%smessageset/%s' % (url, messageset_id),
                        headers=headers)
        r.raise_for_status()

        messageset = r.json()
        schedules[messageset_id] = messageset['default_schedule']

    return schedules[messageset_id]


parser = argparse.ArgumentParser(description='Subscribe PMTCT users to a'
                                 'important message set.')
parser.add_argument('--sbm-url', required=True,
                    help='The url for the Stage Based Messaging service.')
parser.add_argument('--sbm-token', required=True,
                    help='The token for the Stage Based Messaging service.')
parser.add_argument('--file', dest='data_file', type=argparse.FileType('r'),
                    help='Name of file containing the list of identities.')
parser.add_argument('--data', help='List of identities. One per line.')

args = parser.parse_args()
sbm_url = args.sbm_url
sbm_token = args.sbm_token

if args.data_file:
    identity_list = args.data_file.readlines()
elif args.data:
    identity_list = args.data.split("\n")

for item in identity_list:
    identity = json.loads(item)

    old_set_id = identity['current_messageset_id']
    new_set_id = identity['expected_messageset_id']
    old_seq = identity['current_sequence_number']
    new_seq = identity['expected_sequence_number']

    # if this is a PMTCT message set
    if old_set_id in set_details.keys():
        start = None
        end = None

        index = 0
        # Loop through all sets with important messages
        for key, data in set_details.items():

            # loop through ALL messages
            for seq in range(1, 70):

                # If message is important, increase the index
                if seq in data['seq']:
                    index += 1

                # find index where we are starting and ending
                if (key == old_set_id and seq == old_seq):
                    start = index + 1
                if (key == new_set_id and seq == new_seq):
                    end = index

                    # If the next message is important we don't send it
                    if seq in data['seq']:
                        end -= 1

        if start and end and start <= end:
            messageset_id = new_set_ids[end]

            schedule_id = get_messageset_schedule(sbm_url, sbm_token,
                                                  messageset_id)

            data = {
                'identity': identity['identity'],
                'lang': identity['language'],
                'next_sequence_number': start,
                'messageset': messageset_id,
                'schedule': schedule_id
            }
            try:
                create_sub(args.sbm_url, args.sbm_token, data)
            except requests.HTTPError as e:
                sys.stdout.write("Subscription creation failed - Identity: %s "
                                 "Error code: %s\n" % (
                                  identity['identity'], e.response.status_code)
                                 )
                continue
            sys.stdout.write("Subscription created - Identity: %s Set: %s "
                             "Start: %s\n" % (
                                identity['identity'], end, start))

sys.stdout.write("Operation complete\n")
