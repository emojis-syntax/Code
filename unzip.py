#!/usr/bin/python3

# gunzip all ".gz" files in a directory

# Using Multiprocessing to Make Python Code Faster
# See: https://medium.com/@urban_institute/using-multiprocessing-to-make-python-code-faster-23ea5ef996ba

from __future__ import unicode_literals
import re
import os
import sys
from collections import namedtuple, Counter
from os import listdir
from os.path import isfile, join
import multiprocessing
import argparse

# Number of simultaneous processes
N_PROC = 24

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", required=True, help="file directory (required)")
args = parser.parse_args()
directory = args.directory

onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f)) and 'gz' in os.path.splitext(f)[1]]
onlyfiles.sort()
files = onlyfiles[:]

curdir = os.getcwd()
os.chdir(directory)

def multiprocessing_func(fname):
	os.system("gunzip " + fname)

if __name__ == '__main__':
	pool = multiprocessing.Pool(N_PROC)
	try:
		pool.map(multiprocessing_func, files)
	except Exception as e:
		print(e, file=sys.stderr)
	pool.close()
	os.chdir(curdir)


