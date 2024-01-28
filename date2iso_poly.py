#!/usr/bin/python3

# Convert date to ISO

import argparse
import dateutil.parser as dateparser
import json
import ndjson

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", required=True, help="file to read (mandatory)")
args = parser.parse_args()

ifname = args.file
ofname = "__DATE__" + ifname

with open(ifname, 'r') as file:
	data = file.read()
data = ndjson.loads(data)

for idx, tweet in enumerate(data):
	text = tweet['created_at']
	date = dateparser.parse(text)
	data[idx]['created_at'] = date.isoformat()

out = open(ofname, "w")
out.write(json.dumps(data))
out.close()

