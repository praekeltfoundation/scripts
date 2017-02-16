#!/usr/bin/env python

import argparse
import json
import requests
import sys
from collections import OrderedDict

set_details = OrderedDict()
set_details[3] = {'new': 2, 'seq': [8, 15, 24]}
set_details[4] = {'new': 6, 'seq': [2]}
set_details[5] = {'new': 7, 'seq': [5]}
set_details[1] = {'new': 5, 'seq': [6, 15, 17]}

# TODO replace this with the new message set ids created

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

    start = False

    # if this is a PMTCT message set
    if old_set_id in set_details.keys():

        # loop through the message set
        for key, data in set_details.items():

            # if we reach the first set of sub or if we have started
            if key == old_set_id or start:
                start = True
                start_seq = None

                # we loop through important messages
                for i, val in enumerate(data['seq']):

                    # if our current sequence is before a important messages
                    # and our after sequence is after a important message or
                    # we have to move to the next set, we know we have to
                    # subscribe and start at this sequence
                    if old_seq < val and (new_seq >= val or key != new_set_id):
                        start_seq = i + 1
                        break

                # create subscription with on set data['new'] on start_seq
                if start_seq:
                    try:
                        messageset_schedule = get_messageset_schedule(
                            sbm_url, sbm_token, data['new'])
                    except requests.HTTPError as e:
                        sys.exit("Problem retrieving the messageset: %s" %
                                 e.response.status_code)
                        break

                    sub = {
                        'identity': identity['identity'],
                        'lang': identity['language'],
                        'next_sequence_number': start_seq,
                        'messageset': data['new'],
                        'schedule': messageset_schedule
                    }

                    try:
                        create_sub(args.sbm_url, args.sbm_token, sub)
                    except requests.HTTPError as e:
                        sys.stdout.write("Subscription creation failed - "
                                         "Identity: %s Error code: %s\n" %
                                         (identity['identity'],
                                          e.response.status_code))


sys.stdout.write("Operation complete\n")
