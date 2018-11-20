#!/usr/bin/env python

import argparse
import json
import requests
import sys

session = requests.Session()
http = requests.adapters.HTTPAdapter(max_retries=5)
https = requests.adapters.HTTPAdapter(max_retries=5)
session.mount('http://', http)
session.mount('https://', https)


def create_sub(url, token, data):
    headers = {
        'Authorization': "Token " + token,
        'Content-Type': "application/json"
    }
    if execute:
        resp = session.post(
            '%ssubscriptions/' % url, headers=headers, json=data)
        resp.raise_for_status()


def sub_exists(url, token, params):
    headers = {
        'Authorization': "Token " + token,
        'Content-Type': "application/json"
    }
    if execute:
        resp = session.get(
            '%ssubscriptions/' % url, headers=headers, params=params)
        resp.raise_for_status()
        if resp.json().get('count') > 0:
            return True
    return False


def get_messageset_schedule(url, token, messageset_id):
    headers = {
        'Authorization': "Token " + token,
        'Content-Type': "application/json"
    }
    resp = session.get('%smessageset/%s' % (url, messageset_id),
                       headers=headers)
    resp.raise_for_status()
    return resp.json()['default_schedule']


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
parser.add_argument(
    '--execute', default=False, action='store_const', const=True,
    help='Execute the changes, rather than just doing a dry run'
)


args = parser.parse_args()
sbm_url = args.sbm_url
sbm_token = args.sbm_token
messagesets = args.messageset_ids
execute = args.execute

if not execute:
    sys.stdout.write(
        "Dry run mode. If you want the actions to be executed, use --execute"
        "\n")

POSTBIRTH_1_MESSAGESETS = [8, 42]
POSTBIRTH_2_MESSAGESETS = [7, 43]

message_schedules = {}
for messageset_id in messagesets:
    try:
        message_schedules[messageset_id] = get_messageset_schedule(
            sbm_url, sbm_token, messageset_id)
    except requests.HTTPError as e:
        sys.exit("Error retrieving messageset %s: %s" % (messageset_id,
                 e.response.status_code))

if args.data_file:
    identity_list = args.data_file.readlines()
elif args.data:
    identity_list = args.data.split("\n")
else:
    sys.exit("Either --file or --data argument must be present.")

count = 0
for item in identity_list:
    messageset_id = None
    identity = json.loads(item)
    old_set = identity['current_messageset_id']
    new_set = identity['expected_messageset_id']
    old_msg = identity['current_sequence_number']
    new_msg = identity['expected_sequence_number']

    # new position in set 8
    if new_set in POSTBIRTH_1_MESSAGESETS:
        if new_msg > 29 and (
                old_set not in POSTBIRTH_1_MESSAGESETS or old_msg <= 29):
            messageset_id = messagesets[2]  # send 29
        elif new_msg > 21 and (
                old_set not in POSTBIRTH_1_MESSAGESETS or old_msg <= 21):
            messageset_id = messagesets[1]  # send 21
        elif new_msg > 13 and (
                old_set not in POSTBIRTH_1_MESSAGESETS or old_msg <= 13):
            messageset_id = messagesets[0]  # send 13
    # new position in set 7
    elif new_set in POSTBIRTH_2_MESSAGESETS:
        if new_msg > 36 and (
                old_set in POSTBIRTH_2_MESSAGESETS or old_msg <= 36):
            messageset_id = messagesets[3]  # send 36
        # they didn't receive the last message of set 8
        elif old_set not in POSTBIRTH_2_MESSAGESETS and old_set != 8:
            messageset_id = messagesets[2]  # send 29 of 8
        elif old_set in POSTBIRTH_1_MESSAGESETS and old_msg <= 29:
            messageset_id = messagesets[2]  # send 29 of 8

    if messageset_id is None:
        continue
    try:
        if sub_exists(
                args.sbm_url, args.sbm_token, {
                    'identity': identity['identity'],
                    'messageset': messageset_id}):
            sys.stdout.write(
                "Subscription creation skipped - Identity: %s already "
                "subscribed to messageset %s\n" % (
                    identity['identity'], messageset_id))
            continue
    except requests.HTTPError as e:
        sys.stdout.write(
            "Problem retrieving existing subscriptions - Identity: %s Error "
            "code: %s\n" % (identity['identity'], e.response.status_code))
        continue

    data = {
        'identity': identity['identity'],
        'lang': identity['language'],
        'next_sequence_number': 1,
        'messageset': messageset_id,
        'schedule': message_schedules[messageset_id]
    }
    try:
        create_sub(args.sbm_url, args.sbm_token, data)
        count += 1
    except requests.HTTPError as e:
        sys.stdout.write(
            "Subscription creation failed - Identity: %s Error code: %s\n" % (
                identity['identity'],
                e.response.status_code
            )
        )
        continue
    sys.stdout.write(json.dumps({
        "identity": identity["identity"],
        "messageset": messageset_id
    }))
    sys.stdout.write("\n")

sys.stdout.write("Operation complete. %s Subscriptions created.\n" % count)
