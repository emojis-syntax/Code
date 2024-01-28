#!/bin/bash
# #$ -q gpu
# scl enable rh-python36 bash
#. ~/pvenv/bin/activate

FILE=$1
OUT=${FILE}.out
ERR=${FILE}.err

#echo $OUT
#echo $ERR

cd /disco2/keyphrase/stanford-corenlp-full-2018-02-27/

## How to configure slf4j-simple
## See: https://stackoverflow.com/questions/14544991/how-to-configure-slf4j-simple
## Option to suppress logging #433
## See: https://github.com/stanfordnlp/CoreNLP/issues/433
## Mute Stanford coreNLP logging
## See: https://stackoverflow.com/questions/41761099/mute-stanford-corenlp-logging
## Running Stanford CoreNLP server multithreadedly
## See: https://stackoverflow.com/questions/36782543/running-stanford-corenlp-server-multithreadedly
## What is the default number of threads in stanford-corenlp
## https://stackoverflow.com/questions/51636158/what-is-the-default-number-of-threads-in-stanford-corenlp
#java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -mx4g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -preload tokenize,ssplit,pos -status_port 9000 -port 9000 -timeout 15000 -threads 24 -quiet &

cd /disco2/keyphrase/ai-research-keyphrase-extraction/
python3 twitter_preprocessor.py -d $FILE > $OUT 2> $ERR

