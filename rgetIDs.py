#!/usr/bin/python3

# Read all tweet IDs from files in a directory and store them in the _IDs_.txt file within that directory
# The files in this directory must have the json or jsonl extension, and must contain a list of tweets
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

onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f)) and (re.search(r"jsonl?", f))]
onlyfiles.sort()
files = onlyfiles[:]
dfiles = [directory + "/" + s for s in files]

ofname = directory + "/_IDs_.txt" 
out = open(ofname, "w")

for f in dfiles:
	with open(f, 'r') as file:
		data = file.read()
	data = json.loads(data)

	for idx, tweet in enumerate(data):
		id = tweet['id_str']
		out.write(id + "\n")

out.close()

