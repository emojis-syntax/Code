#!/usr/bin/python3

# Counts the number of tweets in a directory
# The files in this directory must have the "jsonl?.out.tw" extension, and must contain a list of tweets
# Receives a directory as a parameter, and processes all files in that directory

import argparse
import json
import re
import os
from os import listdir
from os.path import isfile, join

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", required=True, help="file directory (required)")
args = parser.parse_args()
directory = args.directory

onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f)) and (re.search(r"jsonl?.out.tw$", f))]
onlyfiles.sort()
files = onlyfiles[:]
dfiles = [directory + "/" + s for s in files]

n = 0
for f in dfiles:
	with open(f, 'r') as file:
		data = file.read()
	data = json.loads(data)

	for idx, tweet in enumerate(data):
		n = n+1

print(directory + ": " + str(n) + " tweets")

