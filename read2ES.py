#!/usr/bin/python3

# Program that reads JSON files in the format "jsonl?.out.tw" for ElasticSearch
# receives the ElasticSearch index as a parameter, and the file "jsonl?.out.tw"

import argparse
import json
import os
import re
import sys
from os import listdir
from os.path import isfile, join

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--index", required=True, help="index for ElasticSearch (mandatory)")
parser.add_argument("-d", "--directory", required=True, help="file directory (required)")
args = parser.parse_args()

index = args.index
directory = args.directory

onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f)) and (re.search(r"jsonl?.out.tw$", f) is not None)]
onlyfiles.sort()
files = onlyfiles[:]
dfiles = [directory + "/" + s for s in files]

#print(dfiles); quit()

for ifname in dfiles:
	ofname = ifname + ".TMP"

	with open(ifname, 'r') as file:
		data = file.read()
	data = json.loads(data)

	# Write LIMIT tweets at a time in ElasticSearch, because you can't write the whole file at once
	out = open(ofname, "w")
	ntweet = 0
	LIMIT = 500
	for tweet in data:
		out.write( '{"create":{ "_index":"%s","_id":"%s"}}\n' % (index,tweet['id_str']) )
		out.write(json.dumps(tweet) + "\n")
		#break
		ntweet += 1
		if ntweet % LIMIT == 0:
			out.close()
			cmd = """curl -o /dev/null -H "Content-Type: application/json" -XPUT '127.0.0.1:9200/_bulk?pretty' --data-binary @%s""" % ofname
			os.system(cmd)
			out = open(ofname, "w")
	#	if ntweet >= LIMIT:
	#		break		
	out.close()
	#quit()

	# Adds the last block of tweets, left beyond the multiples of LIMIT tweets
	if ntweet % LIMIT != 0:
		cmd = """curl -o /dev/null -H "Content-Type: application/json" -XPUT '127.0.0.1:9200/_bulk?pretty' --data-binary @%s""" % ofname
		os.system(cmd)

	os.remove(ofname)

