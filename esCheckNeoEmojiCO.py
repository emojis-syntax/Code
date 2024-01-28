#!/usr/bin/python3

# Detects and counts tweets with emojis in a row, and checks if they are also present in Neo4j
# receives as a parameter the ElasticSearch index, and the type of tweet: text, qtext, rtext, any

import argparse
import elasticsearch
import elasticsearch.helpers

from neo4j import GraphDatabase
driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "XXXX"))
session = driver.session()

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--index", required=True, help="index for ElasticSearch (mandatory)")
parser.add_argument("-o", "--origin", required=False, default='text', help="count emojis of the indicated type: text[default], rtext, qtext, any (optional)")

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

kk = 0
for item in results:
	start=0
	if len(item['_source']['emoji']) >= 2:
		s = ""
		e_ant = ""	# emoji anterior
		pos_ant = -1	# posição do emoji anterior
		ocorrencia = []
		for e in item['_source']['emoji']:
			pos = item['_source']['text'].encode('utf-8').find(e.encode('utf-8'), start)
			ln = len(e.encode('utf-8'))
			start = pos + ln
			if pos == pos_ant:
				ocorrencia.append(e_ant + " " + e)

			e_ant = e
			pos_ant = pos + ln
		if len(ocorrencia) != 0:
			es_count = len(ocorrencia)
			cmd =  "MATCH ()-[r:FOLLOWED_BY { index: \"" + index + "\", tweet_id: " + str(item['_source']['id_str']) + " }]->() RETURN SUM(r.count) AS cnt"
			ret = session.run(cmd)
			s = ', '.join(ocorrencia)
			#print(s)
			lista = []
			for val in ret:
				lista.append(val)
			neo_count = lista[0]["cnt"]
			if es_count != neo_count:
				#print("es_count: " + str(es_count) + ", "neo_count: " + str(neo_count))
				#print(s)
				print("id_str: " + str(item['_source']['id_str']) + ", id24: " + str(item['_source']['id24']))
				if kk>20:
					quit()
				kk += 1

print("OK")
