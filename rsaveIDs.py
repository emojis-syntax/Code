#!/usr/bin/python3

# Read all tweet IDs from files in a directory and store them in the <file>_IDs.txt file within that directory
# The files in this directory must have the json or jsonl extension, and must contain a list of tweets or lines of tweets
# Receives a directory as a parameter, and processes all files in that directory

import argparse
import json
import ndjson
import re
import os
import pathlib
from os import listdir
from os.path import isfile, join
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", required=True, help="file directory (required)")
args = parser.parse_args()
directory = args.directory

onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f)) and (re.search(r"jsonl?$", f))]
onlyfiles.sort()
files = onlyfiles[:]
#print(files)
#quit(0)
#dfiles = [directory + "/" + s for s in files]

for df in files:
	f = directory + "/" + df
	with open(f, 'r') as file:
		data = file.read()
	try:
		data = json.loads(data)
		lista = True
	except:
		data = ndjson.loads(data)
		lista = False

	#print(Path(df).stem)
	ofname = directory + "/" + Path(df).stem + "_IDs.txt"
	#print(ofname)
	#if lista:
	#	print("It's a list")
	#else:
	#	print("It's not a list")
	
	out = open(ofname, "w")
	for tweet in data:
		#print(tweet['id_str'])
		id = tweet['id_str']
		out.write(id + "\n")

	out.close()
		
	#break

