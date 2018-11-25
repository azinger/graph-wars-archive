import json

import boto3


STATS_ROOT_PATH_ELEMS = ['stats', 'data']
STATS_ROOT_PATH_LEN = len(STATS_ROOT_PATH_ELEMS)

STATS_CUSTOM_FORMATTERS = {
	'game_key': lambda stats_row, header_indexes: '<a href="{playback_page_url}?gameUrl=/data/raw/{year}/{month}/{day}/{game_key}.json">{game_key}</a>'.format(
		playback_page_url='/playback/index.html',
		year=stats_row[header_indexes['year']],
		month=stats_row[header_indexes['month']],
		day=stats_row[header_indexes['day']],
		game_key=stats_row[header_indexes['game_key']]
	),
}


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
		print('Will return empty list for stats key {stats_key}'.format(stats_key=stats_key))
		return []
	stats_table = json.loads(stats_content)
	return stats_table


def write_stats_html(stats, page_key, s3_client):
	header_indexes = {}
	row_ix = -1
	page_lines = [
		'<html>',
		'<head><title>Graph Wars Archive Listing</title></head>',
		'<body>',
		'<table>'
	]
	for stats_row in stats:
		row_ix += 1

		page_lines += ['<tr>']
		col_ix = -1
		for stats_val in stats_row:
			col_ix += 1
			if row_ix == 0:
				header_indexes[stats_val] = col_ix
			header = stats[0][col_ix]
			if row_ix == 0 or header not in STATS_CUSTOM_FORMATTERS:
				display_val = stats_val
			else:
				display_val = STATS_CUSTOM_FORMATTERS[header](stats_row, header_indexes)
			page_lines += [
				'<td>',
				str(display_val),
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
