#!/usr/bin/python3

# Corrected version, with list of 31 queries

# Using Multiprocessing to Make Python Code Faster
# See: https://medium.com/@urban_institute/using-multiprocessing-to-make-python-code-faster-23ea5ef996ba

# Detect tweets with emojis in a row and load them into neo4j, as co-occurrences
# receives as a parameter the ElasticSearch index, and the type of tweet: text, qtext, rtext, any
# which is also the type of relationship in neo4j

import sys
import argparse
import elasticsearch
import elasticsearch.helpers
import multiprocessing

# Number of simultaneous processes
N_PROC = 24

from neo4j import GraphDatabase
driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "XXXX"))
session = driver.session()

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--index", required=True, help="index for ElasticSearch and type for neo4j relationship (mandatory)")
parser.add_argument("-o", "--origin", required=False, default='text', help="count emojis of the indicated type: text[default], rtext, qtext, any (optional)")

args = parser.parse_args()

index = args.index
orig = args.origin


# Create a list with 31 queries
qlista = []

for i24 in range(N_PROC):
	if orig=='any':
		query={ "query": { "bool": { "must": [ { "match": { "id24": i24} }, { "exists": { "field": "emoji" } } ] } } }
	else:
		query={ "query": { "bool": { "must": [ { "match": { "original_field": orig } }, { "match": { "id24": i24 } }, { "exists": { "field": "emoji" } } ] } } }
	qlista.append(query)

#print(qlista);quit()

def multiprocessing_func(query):
	# See:
	# Python elasticsearch.helpers.scan example
	# https://stackoverflow.com/questions/47722238/python-elasticsearch-helpers-scan-example
	#
	# Elasticsearch scroll timeout
	# As I make a query to get all the records, this may cause a timeout.
	# To do this, you need to use the scroll and timeout parameters in the "elasticsearch.helpers.scan" function
	# https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#scroll-search-context
	# https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#time-units
	# https://github.com/elastic/elasticsearch-dsl-py/issues/796
	# https://stackoverflow.com/questions/33301119/error-with-elasticsearch-helpers-scan-api-when-using-query-parameter

	es = elasticsearch.Elasticsearch()
	results = elasticsearch.helpers.scan(es,
		scroll="30d",
    		index=index,
    		query=query
	)

	for item in results:
		start=0
		if len(item['_source']['emoji']) >= 2:
			s = ""
			e_ant = ""	# previous emoji
			pos_ant = -1	# previous emoji position
			for e in item['_source']['emoji']:
				pos = item['_source']['text'].encode('utf-8').find(e.encode('utf-8'), start)
				ln = len(e.encode('utf-8'))
				start = pos + ln
				if pos == pos_ant:
					cmd =  "MATCH (n1:Emoji { emoji: \"" + e_ant + "\" } )\n"
					cmd += "MATCH (n2:Emoji { emoji: \"" + e + "\" } )\n"
					cmd += "MERGE (n1)-[r1:FOLLOWED_BY { index: \"" + index + "\", tweet_id: " + item['_source']['id_str'] + ", created_at: datetime('" + item['_source']['created_at'] + "') }]->(n2)\n"
					cmd += "SET r1.count = COALESCE(r1.count, 0) + 1;"
					retries = 0
					while True:
						try:
							session.run(cmd)
						except TransientError:
							retries += 1
							print("Concurrence conflict. Retrial no. " + str(retries))
							if retries >= 10:
								raise
						else:
							break
				e_ant = e
				pos_ant = pos + ln

if __name__ == '__main__':
	pool = multiprocessing.Pool(N_PROC)
	try:
		pool.map(multiprocessing_func, qlista)
	except Exception as e:
		print(e, file=sys.stderr)
	pool.close()

driver.close()

