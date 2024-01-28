#!/usr/bin/python3

# Create a KEYPHRASE relationship between emoji co-occurrences and store the arrays of keyphrases and weights there
# receives the ElasticSearch index as a parameter

import argparse
import elasticsearch
import elasticsearch.helpers
import json

# For Neo4j connection timeout, see:
# https://neo4j.com/docs/api/python-driver/current/api.html
# I defined 30 days of timeout (max_connection_lifetime)
#
from neo4j import GraphDatabase
driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "XXXX"), max_connection_lifetime=2592000)
session = driver.session()

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--index", required=True, help="index for ElasticSearch (mandatory)")

args = parser.parse_args()

index = args.index

es = elasticsearch.Elasticsearch()

#cmd =  "MATCH (n {emoji:\"🇺🇸\"})-[r:FOLLOWED_BY { index: \"" + index + "\" }]->(m {emoji:\"😂\"}) RETURN r.tweet_id AS tid"
#cmd =  "MATCH (n {emoji:\"🇺🇸\"})-[r:FOLLOWED_BY { index: \"" + index + "\" }]->(m) RETURN r.tweet_id AS tid"
#cmd =  "MATCH (n {emoji:\"💋\"})-[r:FOLLOWED_BY { index: \"" + index + "\" }]->(m) RETURN m.emoji,r.tweet_id AS tid"
#cmd =  "MATCH (n {emoji:\"💋\"})-[r:FOLLOWED_BY { index: \"" + index + "\" }]->(m) RETURN ID(n) AS n_id,ID(m) AS m_id,r.tweet_id AS tid"
#cmd =  "MATCH (n)-[r:FOLLOWED_BY { index: \"" + index + "\" }]->(m) RETURN ID(n) AS n_id,ID(m) AS m_id,r.tweet_id AS tid ORDER BY n_id,m_id LIMIT 2000"

#cmd2 =  "MATCH (n)-[r:FOLLOWED_BY { index: \"" + index + "\" }]->(m) RETURN DISTINCT ID(n) AS n_id,ID(m) AS m_id ORDER BY n_id,m_id LIMIT 5"
cmd2 =  "MATCH (n)-[r:FOLLOWED_BY { index: \"" + index + "\" }]->(m) RETURN DISTINCT ID(n) AS n_id,ID(m) AS m_id ORDER BY n_id,m_id"
ret2 = session.run(cmd2)
cooc = []
for val2 in ret2:
	cooc.append(val2)
for el2 in cooc:
	n_id = el2["n_id"]
	m_id = el2["m_id"]
	cmd =  "MATCH (n)-[r:FOLLOWED_BY { index: \"" + index + "\" }]->(m) WHERE ID(n)=" + str(n_id) + " AND ID(m)=" + str(m_id) + " RETURN r.tweet_id AS tid"
	ret = session.run(cmd)
	lista = []
	for val in ret:
		lista.append(val)
	#print(lista);quit()
	#for el in lista:
	#	print(el)
	vtext = []
	vweit = []
	vcnt = []
	for el in lista:	
		tid = el["tid"]
		#print(tid)
		query={ "query": { "match": { "id_str": tid } } }
		results = elasticsearch.helpers.scan(es,
			index=index,
			query=query
		)
		for item in results:
			ktext = item['_source']['keyphrase_text']
			kweit = item['_source']['keyphrase_weight']
			if ktext is None:
				continue
			#print(ktext)
			#print(kweit)
			for idx,kt in enumerate(ktext):
				kw = kweit[idx]
				try:
					pos = vtext.index(kt)
					vweit[pos] += kw
					vcnt[pos] += 1
				except ValueError:
					vtext.append(kt)
					vweit.append(kw)
					vcnt.append(1)

	#print("---")
	#print(vtext)
	#print(vweit)
	kall = list(zip(vtext, vweit, vcnt))
	skall = sorted(kall, key=lambda tup: tup[1], reverse=True)
	#print(skall[:30])
	j = [ {"k":i[0], "w":i[1], "c":i[2]} for i in skall[:30]]
	j = json.dumps(j)
	#print(j); quit()
	# From here it's to redo
	cmd =  "MATCH (n:Emoji)\n"
	cmd += "MATCH (m:Emoji)\n"
	cmd += "WHERE ID(n)=" + str(n_id) + " AND ID(m)=" + str(m_id) + "\n"
	cmd += "CREATE (n)-[r1:KEYPHRASES { index: \"" + index + "\", keyphrases: \"" + j.replace('"','\\"') + "\"}]->(m);"
	#print(cmd)
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
	

