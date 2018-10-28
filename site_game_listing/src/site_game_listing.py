import json

import boto3


STATS_ROOT_PATH_ELEMS = ['stats', 'data']
STATS_ROOT_PATH_LEN = len(STATS_ROOT_PATH_ELEMS)


def lambda_handler(event, context):
	print(event)

	s3_client = boto3.client('s3')
	lambda_client = boto3.client('lambda')

	for record in event['Records']:
		bucket_name = record['s3']['bucket']['name']
		s3_key = record['s3']['object']['key']
		dest_s3_key = calc_dest_s3_key(s3_key)

		stats = read_stats(bucket_name, s3_key, s3_client)
		write_stats_html(stats, dest_s3_key, s3_client)


def calc_dest_s3_key(s3_key):
	path = s3_key.split('/')
	dot_ix = path[-1].rfind('.')
	if dot_ix > 0:
		path[-1] = '{}.html'.format(path[-1][:dot_ix])
	return '/'.join(path[2:])


def read_stats(bucket_name, stats_key, s3_client):
	try:
		stats_object = s3_client.get_object(
			Bucket='graph-wars-archive',
			Key=stats_key
		)
		stats_bytes = stats_object['Body'].read()
		stats_content = stats_bytes.decode('UTF-8')
	except Exception as ex:
		print(ex)
		print('Will return empty list for stats.')
		return []
	stats_table = json.loads(stats_content)
	return stats_table


def write_stats_html(stats, page_key, s3_client):
	page_lines = [
		'<html>',
		'<head><title>Graph Wars Archive Listing</title></head>',
		'<body>',
		'<table>'
	]
	for stats_row in stats:
		page_lines += ['<tr>']
		for stats_val in stats_row:
			page_lines += [
				'<td>',
				str(stats_val),
				'</td>'
			]
		page_lines += ['</tr>']
	page_lines += [
		'</table>',
		'</body>',
		'</html>'
	]
	page_content = '\n'.join(page_lines)
	print('Writing index page {}'.format(page_key))
	s3_client.put_object(
		Bucket='graph-wars-archive-www',
		Key=page_key,
		ContentType='text/html; charset=utf-8',
		Body=page_content.encode('UTF-8')
	)
