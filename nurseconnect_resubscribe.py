#!/usr/bin/env python

import argparse
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
    resp = session.post('%ssubscriptions/' % url, headers=headers, json=data)
    resp.raise_for_status()


def sub_exists(url, token, params):
    headers = {
        'Authorization': "Token " + token,
        'Content-Type': "application/json"
    }
    resp = session.get('%ssubscriptions/' % url, headers=headers,
                       params=params)
    resp.raise_for_status()
    if resp.json().get('count') > 0:
        return True
    return False


def get_subs(url, token, params):
    next_url = '%ssubscriptions/' % url
    while next_url != "":
        headers = {
            'Authorization': "Token " + token,
            'Content-Type': "application/json"
        }
        resp = session.get(next_url, headers=headers, params=params)
        resp.raise_for_status()
        for result in resp.json().get('results'):
            yield result
        next_url = resp.json().get('next')


def get_messageset_schedule(url, token, messageset_id):
    headers = {
        'Authorization': "Token " + token,
        'Content-Type': "application/json"
    }
    resp = session.get('%smessageset/%s' % (url, messageset_id),
                       headers=headers)
    resp.raise_for_status()
    return resp.json()['default_schedule']


def get_identity(url, token, identity_id):
    headers = {
        'Authorization': "Token " + token,
        'Content-Type': "application/json"
    }
    resp = session.get('%sidentity/%s' % (url, identity_id),
                       headers=headers)
    resp.raise_for_status()
    if resp.status_code == 404:
        return False
    return True


parser = argparse.ArgumentParser(description='Re-subscribe expired '
                                 'nurseconnect users')
parser.add_argument('--sbm-url', required=True,
                    help='The url for the Stage Based Messaging service.')
parser.add_argument('--sbm-token', required=True,
                    help='The token for the Stage Based Messaging service.')
parser.add_argument('--is-url', required=True,
                    help='The url for the Identity Store.')
parser.add_argument('--is-token', required=True,
                    help='The token for the Identity Store.')
parser.add_argument('--new-messageset', type=int, required=True,
                    help='The id of the new nurseconnect messageset.')
parser.add_argument('--old-messageset', type=int, required=True,
                    help='The id of the old nurseconnect messageset.')

args = parser.parse_args()
new_messageset = args.new_messageset
old_messageset = args.old_messageset
sbm_url = args.sbm_url
sbm_token = args.sbm_token
is_url = args.is_url
is_token = args.is_token

try:
    messageset_schedule = get_messageset_schedule(sbm_url, sbm_token,
                                                  new_messageset)
except requests.HTTPError as e:
    sys.exit("Problem retrieving the messageset: %s" % e.response.status_code)

# get all nurseconnect subscriptions that have lapsed
subscriptions = get_subs(sbm_url, sbm_token, {"messageset": old_messageset,
                                              "completed": True})
count = 0
for sub in subscriptions:
    try:
        # check they aren't already subscribed to the new messageset
        if sub_exists(sbm_url, sbm_token, {"identity": sub['identity'],
                                           "messageset": new_messageset}):
            sys.stdout.write("Subscription creation skipped - Identity: %s "
                             "already subscribed to messageset %s\n" %
                             (sub['identity'], new_messageset))
            continue

        # check they aren't still subscribed to the old messageset
        if sub_exists(sbm_url, sbm_token, {"identity": sub['identity'],
                                           "messageset": old_messageset,
                                           "active": True}):
            sys.stdout.write("Subscription creation skipped - Identity: %s "
                             "still subscribed to old messageset %s\n" %
                             (sub['identity'], old_messageset))
            continue

        # check they haven't opted out
        identity = get_identity(is_url, is_token, sub['identity'])
        msisdns = identity['details'].get('addresses', {}).get('msisdn', {})
        if all(msisdn.get('optedout') for _, msisdn in msisdns.items()):
            sys.stdout.write("Subscription creation skipped - Identity: %s "
                             "optedout\n" % (sub['identity']))
            continue
    except requests.HTTPError as e:
        sys.stdout.write("Problem retrieving identity or subscriptions - "
                         "Subscription: %s Error code: %s\n" %
                         (sub['id'], e.response.status_code))
        continue

    data = {
        'identity': identity['identity'],
        'lang': identity['language'],
        'next_sequence_number': 1,
        'messageset': new_messageset,
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
