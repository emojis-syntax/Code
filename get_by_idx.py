#!/usr/bin/python3

import argparse
import dateutil.parser as dateparser
import json

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", required=True, help="file to read (mandatory)")
parser.add_argument("-i", "--index", required=True, help="index of tweet to get (mandatory)")
args = parser.parse_args()

ifname = args.file
idx = int(args.index)

with open(ifname, 'r') as file:
        data = file.read()
data = json.loads(data)

print(data[idx])
