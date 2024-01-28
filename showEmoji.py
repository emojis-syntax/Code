#!/usr/bin/python3

# Counts the number of tweets with emoji in a file

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

s = ""
for tweet in data:
	if tweet['emoji']:
		if orig=='any' or tweet['original_field']==orig:
			emo = tweet['emoji']
			if isinstance(emo, list):
				emo = ', '.join(emo)
			s = s + emo + ", "

print (s)

