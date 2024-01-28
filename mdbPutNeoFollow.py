#!/usr/bin/python3

# Put relationships of type FOLLOWED_BY, which are in the emojis database (MariaDB), follow table, in Neo4j

# Using Multiprocessing to Make Python Code Faster
# See: https://medium.com/@urban_institute/using-multiprocessing-to-make-python-code-faster-23ea5ef996ba

import mariadb
import sys
import multiprocessing
import time

starttime = time.time()

# Number of simultaneous processes
N_PROC = 24

def multiprocessing_func(N):
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


	# For the Neo4j connection timeout, see:
	# https://neo4j.com/docs/api/python-driver/current/api.html
	#
	from neo4j import GraphDatabase
	driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "XXXX"), max_connection_lifetime=2592000)
	session = driver.session()

	q = "SELECT * FROM follow JOIN emoji e1 ON fol_emo_start=e1.emo_id JOIN emoji e2 ON fol_emo_end=e2.emo_id WHERE fol_id%{0}={1}".format(N_PROC, N)
	cur.execute(q)
	results = cur.fetchall()

	for item in results:
		emoji1 = item[8].decode("utf-8")
		emoji2 = item[15].decode("utf-8")
		#print(emoji1);print(emoji2);quit()
		index = item[5]
		tweet_id = str(item[6])
		created_at = item[4]
		count = str(item[3])
		cmd =  "MATCH (n1:Emoji { emoji: \"" + emoji1 + "\" } )\n"
		cmd += "MATCH (n2:Emoji { emoji: \"" +  emoji2 + "\" } )\n"
		cmd += "CREATE (n1)-[r1:FOLLOWED_BY { index: \"" + index + "\", tweet_id: " + tweet_id + ", created_at: datetime('" + created_at + "'), count: " + count + "}]->(n2)\n"
		#print(cmd);quit();
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
	driver.close()
	conn.close()

if __name__ == '__main__':
	pool = multiprocessing.Pool(N_PROC)
	try:
		pool.map(multiprocessing_func, range(N_PROC))
	except Exception as e:
		print(e, file=sys.stderr)
	pool.close()

print('Total time {} seconds'.format(time.time() - starttime))

