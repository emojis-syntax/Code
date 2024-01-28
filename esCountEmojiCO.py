#!/usr/bin/python3

# Detect and count tweets with emojis in a row
# receives as a parameter the ElasticSearch index, and the type of tweet: text, qtext, rtext, any

import argparse
import elasticsearch
import elasticsearch.helpers

DEBUG = False

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

n_ocorrencias = 0
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
			if DEBUG:
				s = s + "   " + e + "  " + str(pos) + "+" + str(ln)
			if pos == pos_ant:
				ocorrencia.append(e_ant + " " + e)
				if DEBUG:
					r = session.run("MATCH (n) RETURN n")
					for i in r:
						print(i)
					print(cmd);quit()

			e_ant = e
			pos_ant = pos + ln
		if len(ocorrencia) != 0:
			n_ocorrencias += len(ocorrencia)
			if DEBUG:
				s += " COOCORRENCIAS: " + ', '.join(ocorrencia)
		if DEBUG:
			print(s)

print("Total co-ocorrencies: " + str(n_ocorrencias))
