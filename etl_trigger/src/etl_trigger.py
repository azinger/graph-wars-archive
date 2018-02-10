import json
from datetime import datetime, timedelta

import boto3


def lambda_handler(event, context):
	print(event)

	s3_client = boto3.client('s3')

	event_time = datetime.strptime(event['time'], '%Y-%m-%dT%H:%M:%SZ')
	prior_day = event_time - timedelta(1, 0, 0)
	trigger = {
		'prefixes': [
			prior_day.strftime('%Y/%m/%d/json/')
		]
	}

	s3_client.put_object(
		Bucket='graph-wars-archive',
		Key='triggers/etl/{now}.json'.format(
			now=datetime.utcnow().strftime('%Y%m%d%H%M%S%s')
		),
		Body=json.dumps(trigger)
	)
