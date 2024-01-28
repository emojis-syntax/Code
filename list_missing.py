#!/usr/bin/python3

# List .jsonl files that have not yet been transformed into .jsonl.out

from __future__ import unicode_literals
import re
import time
import os
import sys
from collections import namedtuple, Counter
from os import listdir
from os.path import isfile, join
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", required=True, help="file directory (required)")
args = parser.parse_args()
directory = args.directory

onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f)) and 'json' in os.path.splitext(f)[1] and not isfile(join(directory, f + ".out"))]
onlyfiles.sort()
files = onlyfiles[:]
dfiles = [directory + "/" + s for s in files]

#print(dfiles)

for f in dfiles:
	print(f)

