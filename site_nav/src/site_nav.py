import json

import boto3


STATS_ROOT_PATH_ELEMS = ['stats', 'data']


def lambda_handler(event, context):
	print(event)

	s3_client = boto3.client('s3')
	for record in event['Records']:
		bucket_name = record['s3']['bucket']['name']
		s3_key = record['s3']['object']['key']
		process_stat_path(bucket_name, s3_key, s3_client)


def process_stat_path(bucket_name, s3_key, s3_client):
	path_elems = s3_key.split('/')
	src_path_start_ix = len(STATS_ROOT_PATH_ELEMS)
	src_path_end_ix = len(path_elems) - 1
	for path_ix in range(src_path_start_ix, src_path_end_ix):
		src_path_elems = path_elems[:path_ix]
		children = []
		load_results = True
		marker = None
		while load_results:
			list_response = s3_client.list_objects(
				Bucket=bucket_name,
				Prefix='/'.join(src_path_elems),
				Marker=marker
			)
			children += [elem['Key'] for elem in list_response['Contents']]
			load_results = list_response['IsTruncated']
			if load_results:
				marker = children[-1]
		write_index(src_path_elems, children, s3_client)


def write_index(src_path_elems, children, s3_client):
	page_lines = [
		'<html>',
		'<head><title>Graph Wars Archive listing</title></head>',
		'<body>',
		'<ul>'
	]
	for child in children:
		child_path_elems = child.split('/')
		child_path_elem = child_path_elems[len(src_path_elems)]
		page_lines += [
			'<li><a href="{href}">{href}</a></li>'.format(href=child_path_elem)
		]
	page_lines += [
		'</ul>',
		'<body>',
		'</html>'
	]
	page_content = '\n'.join(page_lines)
	page_key = '/'.join(src_path_elems[len(STATS_ROOT_PATH_ELEMS) + 1] + 'index.html')
	print('Writing index page {}'.format(page_key))
	s3_client.put_object(
		Bucket='graph-wars-archive-www',
		Key=page_key,
		Body=page_content.encode('UTF-8')
	)
