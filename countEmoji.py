#!/usr/bin/python3

# Counts the number of tweets with emoji

import argparse
import json
import os

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", required=True, help="file to read (mandatory)")
parser.add_argument("-o", "--origin", required=False, default='any', help="count emojis of the indicated type: text, rtext, qtext, any[default] (optional)")
args = parser.parse_args()

ifname = args.file
orig = args.origin

with open(ifname, 'r') as file:
	data = file.read()
data = json.loads(data)

ntweet = 0
for tweet in data:
	if tweet['emoji']:
		if orig=='any' or tweet['original_field']==orig:
			ntweet += 1

print ("Tweets com emoji: " + str(ntweet))

