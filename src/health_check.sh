#!/bin/bash


MAX_DIFF=300

FILE=/tmp/mqtt_updated.json

STAT=$(stat -c "%Y" $FILE)

NOW=$(date +"%s")

DIFF=$(( $NOW - $STAT ))

echo "FILE:$FILE STAT:$STAT NOW:$NOW = DIFF:$DIFF"

if [[ $DIFF -gt $MAX_DIFF ]] ; then
	echo "FAILED"
	exit 1
else
	exit 0
fi
