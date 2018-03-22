#!/bin/bash

s3_bucket="graph-wars-archive"
s3_key_predicate="stats/def"

aws s3 sync ./def "s3://${s3_bucket}/${s3_key_predicate}"