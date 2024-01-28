#!/usr/bin/python3

# Corrected version, with list of 31 queries

# Using Multiprocessing to Make Python Code Faster
# See: https://medium.com/@urban_institute/using-multiprocessing-to-make-python-code-faster-23ea5ef996ba

# Detect tweets with emojis in a row and load them into neo4j, as co-occurrences
# receives as a parameter the ElasticSearch index, and the type of tweet: text, qtext, rtext, any
# which is also the type of relationship in neo4j

import mariadb
import sys
import argparse
import elasticsearch
import elasticsearch.helpers
import multiprocessing

# Number of simultaneous processes
N_PROC = 24

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
	# Connect to MariaDB Platform
	try:
		conn = mariadb.connect(
			user="root",
			password="",
			host="localhost",
			port=3306,
			database="emojis"
		)
	except mariadb.Error as e:
		print(f"Error connecting to MariaDB Platform: {e}")
		sys.exit(1)
	# Get Cursor
	cur = conn.cursor()
	
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
					# First emoji
					cur.execute("SELECT emo_id FROM emoji WHERE emo_emoji=?", (e_ant,))
					row = cur.fetchone()
					if row is None:
						print("Unknown emoji: " + e_ant)
						quit()
					emoid1 = row[0]
					
					# Second emoji
					cur.execute("SELECT emo_id FROM emoji WHERE emo_emoji=?", (e,))
					row = cur.fetchone()
					if row is None:
						print("Unknown emoji: " + e)
						quit()
					emoid2 = row[0]
					
					# See if a relationship already exists
					cur.execute("SELECT fol_id FROM follow WHERE fol_emo_start={0} AND fol_emo_end={1} AND fol_created_at='{2}' AND fol_index='{3}' AND fol_tweet_id={4}".format(emoid1, emoid2, item['_source']['created_at'], index, item['_source']['id_str']))
					row = cur.fetchone()
					if row is not None:
						folid = row[0]
						cur.execute("UPDATE follow SET fol_count=fol_count+1 WHERE fol_id={0}".format(folid))
						conn.commit()
					else:
						cur.execute("INSERT INTO follow (fol_emo_start, fol_emo_end, fol_created_at, fol_index, fol_tweet_id, fol_count) VALUES ({0},{1},'{2}','{3}',{4},1)".format(emoid1, emoid2, item['_source']['created_at'], index, item['_source']['id_str']))
						conn.commit()
				e_ant = e
				pos_ant = pos + ln
	conn.close()

if __name__ == '__main__':
	pool = multiprocessing.Pool(N_PROC)
	try:
		pool.map(multiprocessing_func, qlista)
	except Exception as e:
		print(e, file=sys.stderr)
	pool.close()


