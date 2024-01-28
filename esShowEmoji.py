#!/usr/bin/python3

# Show emojis from a block of entries in Elasticsearch
# receives as a parameter the ElasticSearch index, and the type of tweet: text, qtext, rtext, any

import argparse
import elasticsearch
import elasticsearch.helpers

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--index", required=True, help="index for ElasticSearch (mandatory)")
parser.add_argument("-o", "--origin", required=False, default='any', help="count emojis of the indicated type: text, rtext, qtext, any[default] (optional)")

args = parser.parse_args()

index = args.index
orig = args.origin

if orig=='any':
	query={ "query": { "exists": { "field": "emoji" } } }
else:
	query={ "query": { "bool": { "must": [ { "match": { "original_field": "text" } }, { "exists": { "field": "emoji" } } ] } } }

# See:
# https://stackoverflow.com/questions/47722238/python-elasticsearch-helpers-scan-example
# Python elasticsearch.helpers.scan example

es = elasticsearch.Elasticsearch()
results = elasticsearch.helpers.scan(es,
    index=index,
    query=query
)

for item in results:
	#print(item['_source']['id_str'], item['_source']['emoji'])
	s = ""
	for e in item['_source']['emoji']:
		s = s + "   " + e
	print(s)

