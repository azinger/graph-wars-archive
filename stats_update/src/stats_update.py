import io
import json

import boto3


def lambda_handler(event, context):
	metadata_s3_keys = event['metadata_s3_keys']
	stats_def = event['stats_def']
	print(metadata_s3_keys)
	print(stats_def)

	s3_client = boto3.client('s3')

	metadata_seq = read_metadata(metadata_s3_keys, s3_client)
	stats_cache = {}
	affected_stats_keys = set()
	for metadata in metadata_seq:
		process_metadata(metadata, stats_def, stats_cache, affected_stats_keys, s3_client)
	for stats_key in affected_stats_keys:
		write_stats(stats_key, stats_cache[stats_key], s3_client)


def read_metadata(metadata_s3_keys, s3_client):
	for metadata_s3_key in metadata_s3_keys:
		metadata_object = s3_client.get_object(
			Bucket='graph-wars-archive',
			Key=metadata_s3_key
		)
		metadata_bytes = metadata_object['Body'].read()
		metadata_content = metadata_bytes.decode('UTF-8')
		metadata = json.loads(metadata_content)
		yield metadata


def write_stats(stats_key, stats, s3_client):
	print('Writing stats to key {}'.format(stats_key))
	stats_table = []
	if stats:
		stats_header = [col for col in stats[0].keys()]
		stats_table.append(stats_header)
		for metadata in stats:
			stats_table.append([metadata[col] for col in stats_header])
	stats_content = json.dumps(stats_table)
	s3_client.put_object(
		Bucket='graph-wars-archive',
		Key=stats_key,
		Body=stats_content.encode('UTF-8')
	)


def read_stats(stats_key, s3_client):
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
	stats = []
	stats_header = None
	stats_table = json.loads(stats_content)
	for stats_vals in stats_table:
		if stats_header is None:
			stats_header = stats_vals
		else:
			stats_row = {}
			for header, val in zip(stats_header, stats_vals):
				stats_row[header] = val
			if stats_row:
				stats.append(stats_row)
	return stats


def find_index_sorted(stats, metadata, sort_def):
	def compare(struct1, struct2):
		comp_list1 = []
		comp_list2 = []
		for sort_spec in sort_def:
			field = sort_spec['field']
			val1 = struct1[field]
			val2 = struct2[field]
			# if field in METADATA_TYPES:
			# 	if val1:
			# 		val1 = METADATA_TYPES[field](val1)
			# 	if val2:
			# 		val2 = METADATA_TYPES[field](val2)
			if sort_spec['direction'] == 'desc':
				val1, val2 = val2, val1
			comp_list1.append(val1)
			comp_list2.append(val2)
		if comp_list1 < comp_list2:
			return -1
		elif comp_list1 > comp_list2:
			return 1
		else:
			return 0
	ix = len(stats)
	while ix > 0:
		stats_row = stats[ix - 1]
		if stats_row['game_key'] == metadata['game_key']:
			return -1
		comparison = compare(stats_row, metadata)
		if comparison < 0:
			break
		ix -= 1
	return ix


def process_metadata(metadata, stats_def, stats_cache, affected_stats_keys, s3_client):
	stats_key = stats_def['path'].format(**metadata)
	if stats_key in stats_cache:
		stats = stats_cache[stats_key]
	else:
		stats = read_stats(stats_key, s3_client)
		stats_cache[stats_key] = stats
	insert_ix = find_index_sorted(stats, metadata, stats_def['sort'])
	limit = stats_def['limit']
	if 0 <= insert_ix < limit:
		print('{} will receive game key {}'.format(stats_key, metadata['game_key']))
		stats.insert(insert_ix, metadata)
		while len(stats) > limit:
			stats.pop()
		affected_stats_keys.add(stats_key)
