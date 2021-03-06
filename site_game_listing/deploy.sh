#!/bin/bash

proj=`pwd`
s3_bucket="graph-wars-archive"
s3_key="code/site_game_listing.zip"
lambda_name="site_game_listing"

aws --region us-east-2 s3 cp "${proj}/dist.zip" "s3://${s3_bucket}/${s3_key}"
aws --region us-east-2 lambda update-function-code --function-name "${lambda_name}" --s3-bucket "${s3_bucket}" --s3-key "${s3_key}"
