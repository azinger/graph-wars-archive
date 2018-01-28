import json

import boto3


def lambda_handler(event, context):
	metadata_s3_keys = event['metadata_s3_keys']
	stats_def = event['stats_def']
	print(metadata_s3_keys)
	print(stats_def)

