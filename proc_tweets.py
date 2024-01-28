#!/usr/bin/python3

# Using Multiprocessing to Make Python Code Faster
# See: https://medium.com/@urban_institute/using-multiprocessing-to-make-python-code-faster-23ea5ef996ba

# Process tweets from a file and:
# - Puts the date in ISO format
# - Adds the qtext and rtext fields

import argparse
import dateutil.parser as dateparser
import json
import ndjson
import os
import re
import sys
from os import listdir
from os.path import isfile, join
import multiprocessing

# Number of simultaneous processes
N_PROC = 24

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", required=True, help="file directory (required)")
args = parser.parse_args()
directory = args.directory

onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f))  and (re.search(r"jsonl?$", f) is not None) and not isfile(join(directory, f + ".out"))]
onlyfiles.sort()
files = onlyfiles[:]

def multiprocessing_func(f):
	ifname = directory + "/" + f
	ofname = ifname

	with open(ifname, 'r') as file:
		data = file.read()

	if type(data) is list:
		print("File already processed: " + f)
		return

	data = ndjson.loads(data)

	tweets = []
	for idx, tweet in enumerate(data):
		text = tweet['created_at']
		date = dateparser.parse(text)
		#data[idx]['created_at'] = date.isoformat()

		twj = tweet
		quotetext = ''
		retweettext = ''
		tweettext = ''
		replytotweetid = ''
		retweeted_status_id = ''
		quoted_status_id = ''
		if len(twj) > 1:

			is_quote = False
			is_retweet = False
			if 'retweeted_status' not in twj and 'quoted_status' not in twj:
				tweettext = twj['full_text']
			elif 'quoted_status' in twj:
				tweettext = twj['full_text']
				quotetext = twj['quoted_status']['full_text']
				quoted_status_id = twj['quoted_status']['id_str'] #new
				is_quote = True
			else:
				retweettext = twj['retweeted_status']['full_text']
				is_retweet = True
				retweeted_status_id = twj['retweeted_status']['id_str'] #new

			if twj['in_reply_to_status_id_str'] != None :
				replytotweetid = twj['in_reply_to_status_id_str']

			d = {
				'created_at': date.isoformat(),
				'id_str': twj['id_str'],
				'text': tweettext,
				'qtext': quotetext,
				'rtext': retweettext,
				'user_name': twj['user']['screen_name'],
				'user_followers': twj['user']['followers_count'],
				'user_location' : twj['user']['location'],
				'is_retweet' : is_retweet,
				'reply_to_tweet_id' : replytotweetid,
				'retweet_count': twj['retweet_count'],
				'favorite_count': twj['favorite_count'],
				'lang': twj['lang'],
				'geo': twj['geo'],
				'quoted_status_id': quoted_status_id,
				'retweeted_status_id': retweeted_status_id}
			if twj['place'] != None:
				d['place'] = twj['place']['full_name']
			else:
				d['place'] = 'unknown'
			tweets.append(d) # chnaging here to twj to get entire tweet to debug

	out = open(ofname, "w")
	out.write(json.dumps(tweets))
	out.close()


if __name__ == '__main__':
        pool = multiprocessing.Pool(N_PROC)
        try:
                pool.map(multiprocessing_func, files)
        except Exception as e:
                print(e, file=sys.stderr)
        pool.close()

