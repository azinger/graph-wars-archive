#!/bin/bash

proj=`pwd`
s3_bucket="graph-wars-archive-www"

aws s3 rm "s3://${s3_bucket}/playback" --recursive
aws s3 cp build "s3://${s3_bucket}/playback" --recursive
