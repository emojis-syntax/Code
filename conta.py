#!/usr/bin/python3

# Count tweets

import argparse
import dateutil.parser as dateparser
import json

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", required=True, help="file to read (mandatory)")
args = parser.parse_args()

ifname = args.file

with open(ifname, 'r') as file:
        data = file.read()
data = json.loads(data)

n = 0
for idx, tweet in enumerate(data):
        n = n+1

print(str(n) + " tweets")

