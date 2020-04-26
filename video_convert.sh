#!/bin/bash
DIR=""
# check if directory exists
if [ -d "$DIR" ]; then
    echo "Using $DIR"
else
    mkdir $DIR
fi

ffmpeg -i $1 -vcodec h264 -acodec aac $2
rm $1


