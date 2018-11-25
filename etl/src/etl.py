import json
import os
import re
from datetime import datetime
import urllib

from google.cloud import storage
from google import auth as gauth

import boto3


gcs_path_regex = re.compile(r'/b/gw_archive/o/(.*)')
gcs_name_regex = re.compile(r'(\d+)/(\d+)/(\d+)/json/([^.]+)\.json')
quadrant_regex = re.compile(r'(Top|Bottom)(Left|Right)')


def lambda_handler(event, context):
	print(event)

	s3_client = boto3.client('s3')

	priv_key_loc = '{}/creds.json'.format(os.environ['LAMBDA_TASK_ROOT'])
	scopes = ['https://www.googleapis.com/auth/devstorage.read_only']
	os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = priv_key_loc
	credentials, project = gauth.default()
	if credentials.requires_scopes:
		credentials = credentials.with_scopes(scopes)
	gcs_client = storage.Client(credentials=credentials)

	prefix_seq = read_prefixes_from_trigger(event, s3_client)
	game_seq = read_games_from_gcs(prefix_seq, gcs_client)
	write_games_to_s3(game_seq, s3_client)


def read_prefixes_from_trigger(event, s3_client):
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
		for prefix in trigger['prefixes']:
			yield prefix


def read_games_from_gcs(prefix_seq, gcs_client):
	bucket = gcs_client.bucket('gw_archive')
	for prefix in prefix_seq:
		print('Processing GCS prefix: {prefix}'.format(
			prefix=prefix
		))
		for blob in bucket.list_blobs(prefix=prefix):
			parsed_gcs_path = gcs_path_regex.match(blob.path)
			if not parsed_gcs_path:
				print('Could not match path {path} from GCS blob: {blob}'.format(
					path=blob.path,
					blob=blob
				))
				continue
			gcs_name_urlencoded, = parsed_gcs_path.groups()
			gcs_name = urllib.parse.unquote_plus(gcs_name_urlencoded)
			parsed_gcs_name = gcs_name_regex.match(gcs_name)
			if not parsed_gcs_name:
				print('Could not match name {name} from GCS blob: {blob}'.format(
					name=gcs_name,
					blob=blob
				))
				continue
			year, month, day, game_key = parsed_gcs_name.groups()
			game_js_bytes = blob.download_as_string()
			game_js = str(game_js_bytes, 'UTF-8')
			game = json.loads(game_js)
			metadata = {
				'year': year,
				'month': month,
				'day': day,
				'game_key': game_key,
				'ingest_time': datetime.utcnow().isoformat(),
				'width': game['width'],
				'height': game['height'],
				'moves_per_turn': game['movesPerTurn'],
				'winner': game['events'][-1]['player'],
				'area': game['width'] * game['height'],
				'speed': game['movesPerTurn'] / max(game['width'], game['height']),
				'squareness': min(game['width'], game['height']) / max(game['width'], game['height']),
				'home0': None,
				'home1': None,
				'home2': None,
				'home3': None,
				'player_count': 0,
				'winning_turn_count': 0
			}
			prev_event = None
			for event in game['events']:
				stage = event['stage']
				if stage == 'Home':
					metadata['home{}'.format(metadata['player_count'])] = event['quadrant']
					metadata['player_count'] += 1
				elif stage == 'Outpost':
					if event['player'] != prev_event['player']:
						metadata['winning_turn_count'] += 1
				prev_event = event
			if metadata['player_count'] == 2:
				quadrant_parsed0 = quadrant_regex.match(metadata['home0'])
				quadrant_parsed1 = quadrant_regex.match(metadata['home1'])
				if not quadrant_parsed0 or not quadrant_parsed1:
					print('Could not determine player arrangement for {}'.format(metadata['homes']))
					continue
				vertical0, horizontal0 = quadrant_parsed0.groups()
				vertical1, horizontal1 = quadrant_parsed1.groups()
				if vertical0 == vertical1 or horizontal0 == horizontal1:
					metadata['player_arrangement'] = 'same_side'
				else:
					metadata['player_arrangement'] = 'diagonal'
			else:
				metadata['player_arrangement'] = 'group'
			print('Game metadata: {}'.format(metadata))
			yield game, metadata


def write_games_to_s3(game_seq, s3_client):
	metadata_s3_keys = []
	for game, metadata in game_seq:
		raw_s3_key = 'data/raw/{year}/{month}/{day}/{game_key}.json'.format(
			year=metadata['year'],
			month=metadata['month'],
			day=metadata['day'],
			game_key=metadata['game_key']
		)
		s3_client.put_object(
			Bucket='graph-wars-archive-www',
			Key=raw_s3_key,
			Body=json.dumps(game).encode('UTF-8')
		)
		
		metadata_s3_key = 'data/meta/{year}/{month}/{day}/{game_key}.json'.format(
			year=metadata['year'],
			month=metadata['month'],
			day=metadata['day'],
			game_key=metadata['game_key']
		)
		s3_client.put_object(
			Bucket='graph-wars-archive',
			Key=metadata_s3_key,
			Body=json.dumps(metadata).encode('UTF-8')
		)
		metadata_s3_keys += metadata_s3_key,

	trigger = {
		'metadata_s3_keys': metadata_s3_keys
	}
	s3_client.put_object(
		Bucket='graph-wars-archive',
		Key='triggers/stats_dispatcher/{now}.json'.format(
			now=datetime.utcnow().strftime('%Y%m%d%H%M%S%s')
		),
		Body=json.dumps(trigger).encode('UTF-8')
	)
