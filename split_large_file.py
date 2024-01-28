#!/usr/bin/python3

# Separate each file in a directory into a batch of smaller files
# Receives a directory as a parameter, and processes all files in that directory
# Also receives the number of tweets that each file should contain

import argparse
from datetime import datetime
import json
import ndjson
import re
import os
from os import listdir
from os.path import isfile, join, isdir

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", required=True, help="file directory (required)")
parser.add_argument("-s", "--size", required=True, help="number of tweets per file")
args = parser.parse_args()
directory = args.directory
size = int(args.size)

target = directory + "-split"
if not os.path.exists(target):
	os.makedirs(target)

onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f)) and 'json' in os.path.splitext(f)[1] and not isfile(join(directory, f + ".out"))]
onlyfiles.sort()
files = onlyfiles[:]
#dfiles = [directory + "/" + s for s in files]

for f in files:
	ifname = directory + "/" + f

	# Remove the extension from the file name
	ext = os.path.splitext(f)[1]
	f_no_ext = f.replace(ext, "")

	# Open the file, load the tweets, and place them into files
	n = 0
	ini = 0
	res = []
	with open(ifname, 'r') as file:
		for line in file:
			#data = ndjson.loads(line)
			n = n+1
			res.append(line)
			if n>=size:
				ofname = target + "/" + f_no_ext + "_" + str(ini) + "_" + str(size) + ext
				out = open(ofname, "w")
				#out.write(json.dumps(res))
				for e in res:
					out.write(e)
				out.close()
				n = 0
				res = []
				ini += size
				
		if n != 0:
			ofname = target + "/" + f_no_ext + "_" + str(ini) + "_" + str(n) + ext
			out = open(ofname, "w")
			#out.write(json.dumps(res))
			for e in res:
				out.write(e)
			out.close()

