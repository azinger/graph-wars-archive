import json

import boto3


def lambda_handler(event, context):
	print(event)

	s3_client = boto3.client('s3')
	lambda_client = boto3.client('lambda')

	metadata_s3_keys = read_metadata_s3_keys_from_trigger(event, s3_client)
	print('s3 keys: {}'.format(metadata_s3_keys))

	stats_def_seq = read_stats_def_seq(s3_client)
	invoke_stats_updates(metadata_s3_keys, stats_def_seq, lambda_client)


def read_metadata_s3_keys_from_trigger(event, s3_client):
	for record in event['Records']:
		bucket_name = record['s3']['bucket']['name']
		object_key = record['s3']['object']['key']
		object_details = s3_client.get_object(
			Bucket=bucket_name,
			Key=object_key
		)
		print(object_details)
		trigger_bytes = object_details['Body'].read()
		trigger_content = trigger_bytes.decode('UTF-8')
		trigger = json.loads(trigger_content)
		return trigger['metadata_s3_keys']


def read_stats_def_seq(s3_client):
	stats_def_list_response = s3_client.list_objects(
		Bucket='graph-wars-archive',
		Prefix='stats/def/'
	)
	print('stats_def_list_response: {}'.format(stats_def_list_response))
	for stats_def_response in stats_def_list_response['Contents']:
		stats_def_key = stats_def_response['Key']
		stats_def_object = s3_client.get_object(
			Bucket='graph-wars-archive',
			Key=stats_def_key
		)
		stats_def_bytes = stats_def_object['Body'].read()
		stats_def_content = stats_def_bytes.decode('UTF-8')
		stats_def = json.loads(stats_def_content)
		yield stats_def


def invoke_stats_updates(metadata_s3_keys, stats_def_seq, lambda_client):
	metadata_s3_keys_bytes = json.dumps(metadata_s3_keys).encode('UTF-8')
	for stats_def in stats_def_seq:
		lambda_param = {
			'metadata_s3_keys': metadata_s3_keys,
			'stats_def': stats_def
		}
		lambda_client.invoke(
			FunctionName='stats_update',
			InvocationType='Event',
			Payload=json.dumps(lambda_param).encode('UTF-8')
		)
