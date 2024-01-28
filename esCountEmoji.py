#!/usr/bin/python3

# Show total emojis from a block of entries in Elasticsearch
# receives as a parameter the ElasticSearch index, and the type of tweet: text, qtext, rtext, any

import argparse
import elasticsearch
import elasticsearch.helpers

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--index", required=True, help="index for ElasticSearch (mandatory)")
parser.add_argument("-o", "--origin", required=False, default='any', help="count emojis of the indicated type: text, rtext, qtext, any[default] (optional)")
parser.add_argument("-s", "--size", required=False, default=10000, help="number of emojis in the list [default: 10000 ] (optional)")

args = parser.parse_args()

index = args.index
orig = args.origin
num = args.size

if orig=='any':
	query={ "aggs": { "countfield": { "terms": { "field": "emoji.keyword", "size": num, "min_doc_count": 1 } } } }
else:
	query={ "query": { "match": { "original_field": orig } }, "aggs": { "countfield": { "terms": { "field": "emoji.keyword", "size": num, "min_doc_count": 1 } } } }

# See:
# python "elasticsearch.helpers" doc
# https://stackoverflow.com/questions/63305841/elasticsearch-aggregation-on-large-dataset

es = elasticsearch.Elasticsearch()
results = es.search(
    index=index,
    body=query
)

#print(results["aggregations"]); quit()

for item in results["aggregations"]["countfield"]["buckets"]:
	print(item['key'] + " - " + str(item['doc_count']))

