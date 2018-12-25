import itertools
import json

import boto3


STATS_ROOT_PATH_ELEMS = ['stats', 'data']
STATS_ROOT_PATH_LEN = len(STATS_ROOT_PATH_ELEMS)


class PathTree(object):
	def __init__(self, elem):
		self._elem = elem
		self._children = {}

	def __iter__(self):
		if self._children:
			for child in self._children.values():
				for child_path in child:
					yield (self._elem,) + child_path
		else:
			yield (self._elem,)

	def add_child_path(self, child_path):
		if len(child_path) == 0:
			return
		next_elem = child_path[0]
		remaining_elems = child_path[1:]
		if not next_elem in self._children:
			self._children[next_elem] = PathTree(next_elem)
		self._children[next_elem].add_child_path(remaining_elems)

	def get(self, child_path):
		if len(child_path) == 0:
			return self
		next_elem = child_path[0]
		child_tree = self._children.get(next_elem)
		if child_tree is None:
			return None
		return child_tree.get(child_path[1:])

	def get_child_elems(self):
		return self._children.keys()


def lambda_handler(event, context):
	print(event)

	s3_client = boto3.client('s3')
	lambda_client = boto3.client('lambda')

	invoke_site_game_listing(event, lambda_client)

	for record in event['Records']:
		bucket_name = record['s3']['bucket']['name']
		s3_key = record['s3']['object']['key']
		process_stat_path(bucket_name, s3_key, s3_client)


def process_stat_path(bucket_name, s3_key, s3_client):
	print('Processing {}'.format(s3_key))
	path_elems = s3_key.split('/')
	src_path_start_ix = STATS_ROOT_PATH_LEN
	src_path_end_ix = len(path_elems)

	stats_dir = PathTree('')
	load_results = True
	marker = None
	while load_results:
		if marker:
			list_response = s3_client.list_objects(
				Bucket=bucket_name,
				Prefix='/'.join(STATS_ROOT_PATH_ELEMS),
				Marker=marker
			)
		else:
			list_response = s3_client.list_objects(
				Bucket=bucket_name,
				Prefix='/'.join(STATS_ROOT_PATH_ELEMS)
			)
		for response_content in list_response['Contents']:
			key = response_content['Key']
			path = key.split('/')
			stats_dir.add_child_path(path)
		load_results = list_response['IsTruncated']
		if load_results:
			marker = children[-1]

	for path_ix in range(src_path_start_ix, src_path_end_ix):
		src_path_elems = path_elems[:path_ix]
		nav_dir = stats_dir.get(src_path_elems)
		children = map(html_extension, nav_dir.get_child_elems())
		write_index(src_path_elems, children, s3_client)


def html_extension(filename):
	ext_ix = filename.rfind('.')
	if ext_ix > 0:
		return '{}.html'.format(filename[:ext_ix])
	else:
		return filename


def filename_to_link_text(filename):
	ext_ix = filename.rfind('.')
	if ext_ix > 0:
		link_text = filename[:ext_ix]
	elif ext_ix == 0:
		link_text = filename[1:]
	else:
		link_text = filename
	return link_text.replace('_', ' ')


def write_index(src_path_elems, children, s3_client):
	print('Writing index for {}: {}'.format(src_path_elems, children))
	page_lines = [
		'<html>',
		'<head><title>Graph Wars Archive listing</title></head>',
		'<body>',
		'<ul>'
	]
	for child_path_elem in children:
		page_lines += [
			'<li><a href="{href}">{href_text}</a></li>'.format(
				href=child_path_elem,
				href_text=filename_to_link_text(child_path_elem)
			)
		]
	page_lines += [
		'</ul>',
		'<body>',
		'</html>'
	]
	page_content = '\n'.join(page_lines)
	page_key = '/'.join(src_path_elems[len(STATS_ROOT_PATH_ELEMS):] + ['index.html'])
	print('Writing index page {}'.format(page_key))
	s3_client.put_object(
		Bucket='graph-wars-archive-www',
		Key=page_key,
		ContentType='text/html; charset=utf-8',
		Body=page_content.encode('UTF-8')
	)


def invoke_site_game_listing(event, lambda_client):
	lambda_client.invoke(
		FunctionName='site_game_listing',
		InvocationType='Event',
		Payload=json.dumps(event).encode('UTF-8')
	)
