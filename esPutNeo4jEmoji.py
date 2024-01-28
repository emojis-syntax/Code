#!/usr/bin/python3

# Put the emojis in neo4j, by index, and with the occurrences in that index
# also indicates the name of this emoji

import argparse
import elasticsearch
import json
import os
import re
import sys

# emoji extraction
# code adapted. credit to Elias Dabbas
# https://www.kaggle.com/eliasdabbas/how-to-create-a-python-regex-to-extract-emoji

emoji_source = './emoji-test.txt'

def load_current_emoji(emoji_source):
    try:
        with open(emoji_source, 'rt') as file:
            emoji_raw = file.read()
            return emoji_raw
    except OSError:
        print('Could not open file', emoji_source)
        return -1
    else:
        print('Something went wrong')

def generate_emoji_list(emoji_source):
    emoji_raw = load_current_emoji(emoji_source)
    #EmojiEntry = namedtuple('EmojiEntry', ['codepoint', 'status', 'emoji', 'name', 'group', 'sub_group'])
    E_regex = re.compile(r' ?E\d+\.\d+ ')  # remove the pattern E<digit(s)>.<digit(s)>
    emoji_entries = []

    for line in emoji_raw.splitlines()[32:]:  # skip the explanation lines
        if line == '# Status Counts':  # the last line in the document
            break
        if 'subtotal:' in line:  # these are lines showing statistics about each group, not needed
            continue
        if not line:  # if it's a blank line
            continue
        if line.startswith('#'):  # these lines contain group and/or sub-group names
            if '# group:' in line:
                group = line.split(':')[-1].strip()
            if '# subgroup:' in line:
                subgroup = line.split(':')[-1].strip()
        if group == 'Component':  # skin tones, and hair types, skip, as mentioned above
            continue  # maybe this continue is because of greedy regex or because compound emoji have it already
        if re.search('^[0-9A-F]{3,}', line):  # if the line starts with a hexadecimal number (an emoji code point)
            # here we define all the elements that will go into emoji entries
            codepoint = line.split(';')[0].strip()  # in some cases it is one and in others multiple code points
            status = line.split(';')[-1].split()[0].strip()  # status: fully-qualified, minimally-qualified, unqualified
            if line[-1] == '#':
                # The special case where the emoji is actually the hash sign "#". In this case manually assign the emoji
                if 'fully-qualified' in line:
                    emoji = '#️⃣'
                else:
                    emoji = '#⃣'  # they look the same, but are actually different
            else:  # the default case
                emoji = line.split('#')[-1].split()[0].strip()  # the emoji character itself
            if line[-1] == '#':  # (the special case)
                name = '#'
            else:  # extract the emoji name
                split_hash = line.split('#')[1]
                rm_capital_E = E_regex.split(split_hash)[1]
                name = rm_capital_E
            #templine = EmojiEntry(codepoint=codepoint, status=status, emoji=emoji,  name=name, group=group, sub_group=subgroup)
            data = {}
            data['emoji'] = emoji
            data['name'] = name
            emoji_entries.append(data)

    return emoji_entries


emojis = generate_emoji_list(emoji_source)
#print(emojis); quit()

from neo4j import GraphDatabase
driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "XXXX"))
session = driver.session()

DEBUG = False

def getEmoji(key):
	for emoji in emojis:
		if key==emoji['emoji']:
			return emoji
	return None

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--index", required=True, help="index for ElasticSearch and type for neo4j relationship (mandatory)")
parser.add_argument("-o", "--origin", required=False, default='text', help="use tweet type: text[default], rtext, qtext,  (optional)")

args = parser.parse_args()

index = args.index
orig = args.origin

query={ "query": { "bool": { "must": [ { "match": { "original_field": orig } } ] } } , "size": 0, "aggs": { "topics": { "terms": { "field": "emoji.keyword", "size": 10000 } } } }

# See:
# https://stackoverflow.com/questions/47722238/python-elasticsearch-helpers-scan-example
# Python elasticsearch.helpers.scan example

# See:
# https://stackoverflow.com/questions/28287261/connection-timeout-with-elasticsearch
# Connection Timeout with Elasticsearch

es = elasticsearch.Elasticsearch(timeout=300, max_retries=10, retry_on_timeout=True)
results = es.search(index=index, body=query, size=0)

errors = []
lista = []
#for item in results["aggregations"]["topics"]["buckets"]:
results =  results["aggregations"]["topics"]["buckets"]
for item in results:
	#print(item)
	e = getEmoji(item['key'])
	if e is not None:
		item['name'] = e['name']
		lista.append(item)
	else:
		errors.append(item['key'])

if len(errors)>0:
	print("Atenção: há emojis desconhecidos, por favor atualizar o ficheiro " + emoji_source)
	quit()

#print(results)
#print(lista)
#print(errors)
#quit()

for item in results:
	cmd =  "MERGE (n:Emoji { emoji: \"" + item["key"] + "\", name: \"" + item["name"] + "\" } )\n"
	cmd +=  "SET n." + index + "=" + str(item["doc_count"]) + ";"
	#print(cmd); quit()
	session.run(cmd)

driver.close()
