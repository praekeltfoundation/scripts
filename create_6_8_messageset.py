#!/usr/bin/env python

import argparse
import requests
import urllib
import datetime

IMPORT_DIR = "import_files"
DOWNLOAD = False
CREATE = True

parser = argparse.ArgumentParser(description='create 6-8pm message sets.')
parser.add_argument('--sbm-url', required=True,
                    help='The url for the Stage Based Messaging service.')
parser.add_argument('--sbm-token', required=True,
                    help='The token for the Stage Based Messaging service.')

args = parser.parse_args()
sbm_url = args.sbm_url
sbm_token = args.sbm_token

session = requests.Session()
http = requests.adapters.HTTPAdapter(max_retries=5)
https = requests.adapters.HTTPAdapter(max_retries=5)
session.mount('http://', http)
session.mount('https://', https)


def get_messagesets(url, token):
    headers = {
        'Authorization': "Token " + token,
        'Content-Type': "application/json"
    }
    r = session.get('%smessageset/' % (url),
                    headers=headers)
    r.raise_for_status()

    messagesets = r.json()
    return messagesets['results']


def get_or_create_messageset(url, token, messageset):
    headers = {
        'Authorization': "Token " + token,
        'Content-Type': "application/json"
    }
    r = session.get(
        '%smessageset/?short_name=%s' % (url, messageset['short_name']),
        headers=headers)

    r.raise_for_status()

    result = r.json()
    if result['count'] > 0:
        return result['results'][0]['id']
    else:
        r = session.post(
            '%smessageset/' % (url),
            json=messageset,
            headers=headers)

        return r.json()['id']


def get_messages(url, token, set_id):
    headers = {
        'Authorization': "Token " + token,
        'Content-Type': "application/json"
    }
    r = session.get('%smessageset/%s/messages' % (url, set_id),
                    headers=headers)
    r.raise_for_status()

    messages = r.json()
    return messages['messages']

sets = get_messagesets(sbm_url, sbm_token)

opener = urllib.URLopener()

for messageset in sets:
    name = messageset['short_name']
    if name.find("audio") != -1 and name.find("9_11") != -1:
        new_name = name.replace("9_11", "6_8")

        f = open('%s/%s.csv' % (IMPORT_DIR, new_name), 'w')
        f.write(
            "messageset,sequence_number,lang,text_content,binary_content\n")

        new_id = 0
        if CREATE:
            # TODO: correct schedule_id
            # TODO: next set ?? - maybe just update these two manually

            new_set = {
                'short_name': new_name,
                'default_schedule': messageset['default_schedule'],
                'content_type': messageset['content_type'],
            }

            if "channel" in messageset:
                new_set['channel'] = messageset['channel']

            new_id = get_or_create_messageset(sbm_url, sbm_token, new_set)

        messages = get_messages(sbm_url, sbm_token, messageset['id'])

        for message in messages:
            filename = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f.mp3")
            if DOWNLOAD:
                opener.retrieve(
                    message['binary_content']['content'],
                    '%s/%s' % (IMPORT_DIR, filename))

            line = '%s,%s,%s,%s,%s' % (
                new_id,
                message['sequence_number'],
                message['lang'],
                message['text_content'].replace(',', ''),
                filename)

            f.write('%s\n' % line)
            # break

        f.close()

        # break
