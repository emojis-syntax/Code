#!/bin/bash

CMD=`basename "$0"`
#SCRIPT="qsub go.sh"
SCRIPT="./go.sh"

if [ -z "$1" ]
  then
    echo "USAGE: ./$CMD <dir> "
    exit
fi
DIR=$1

if [ ! -d "$DIR" ]; then
        echo "Directory does not exist"
        exit
fi

FOUT="index.txt"

for FILE in "$DIR"/*
do
        #echo $FILE
        ldir=${#DIR}
        ldir=$(($ldir + 1))
        lfile=${#FILE}
        #lfile=$(($file - $ldir))
        #echo ${FILE:$ldir:$lfile}
        #echo ${FILE:$ldir}
        IN=${FILE:$ldir}
        #OUT=$(./escreveEcra.sh $FILE)
        OUT=$($SCRIPT $FILE)
        #echo "Saída: ${OUT}"
        echo -e "${IN}\t${OUT}" >> $FOUT
done

