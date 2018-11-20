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
parser.add_argument(
    '--execute', default=False, action='store_const', const=True,
    help='Execute the changes, rather than just doing a dry run'
)

args = parser.parse_args()
messageset_id = args.messageset_id
sbm_url = args.sbm_url
sbm_token = args.sbm_token
execute = args.execute

if not execute:
    sys.stdout.write(
        "Dry run mode. If you want the actions to be executed, use --execute"
        "\n")

if args.data_file:
    identity_list = args.data_file.readlines()
elif args.data:
    identity_list = args.data.split("\n")
else:
    sys.exit("Either --file or --data argument must be present.")

try:
    messageset_schedule = get_messageset_schedule(sbm_url, sbm_token,
                                                  messageset_id)
except requests.HTTPError as e:
    sys.exit("Problem retrieving the messageset: %s" % e.response.status_code)

count = 0
for item in identity_list:
    identity = json.loads(item)

    try:
        if sub_exists(args.sbm_url, args.sbm_token,
                      {'identity': identity['identity'],
                       'messageset': messageset_id}):
            sys.stdout.write("Subscription creation skipped - Identity: %s "
                             "already subscribed to messageset %s\n" %
                             (identity['identity'], messageset_id))
            continue
    except requests.HTTPError as e:
        sys.stdout.write("Problem retrieving existing subscriptions - "
                         "Identity: %s Error code: %s\n" %
                         (identity['identity'], e.response.status_code))
        continue
    data = {
        'identity': identity['identity'],
        'lang': identity['language'],
        'next_sequence_number': 1,
        'messageset': messageset_id,
        'schedule': messageset_schedule
    }
    try:
        create_sub(args.sbm_url, args.sbm_token, data)
        count += 1
    except requests.HTTPError as e:
        sys.stdout.write("Subscription creation failed - Identity: %s Error "
                         "code: %s\n" % (identity['identity'],
                                         e.response.status_code))
sys.stdout.write("Operation complete. %s Subscriptions created.\n" % count)
