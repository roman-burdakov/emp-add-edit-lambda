import base64
import datetime
import time
import decimal
import json
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
import requests

def load_config():
    with open('params.json', 'r') as conf_file:
        conf_json = conf_file.read()
        return json.loads(conf_json)

def lambda_handler(event, context):
    #Load config
    config = load_config()
    host = config["es_host"]
    es = Elasticsearch(hosts=[{'host': host, 'port': 443}], use_ssl=True,
        http_compress = True, connection_class=RequestsHttpConnection)
    # print(event)

    response = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin" : "*",
            "Access-Control-Allow-Credentials" : True,
            "Content-Type": "application/json"
        }
    }
    record = {}
    try:
        print('event', event)
        print('type', type(event))
        if 'body' in event and isinstance(event['body'], str):
            record = json.loads(event['body'])
        else:
            record = event['body']
        print('Record type', type(record))
        space = record['space']

        now_ts = time.time()
        processed_timestamp = decimal.Decimal(now_ts)
        date = datetime.datetime.fromtimestamp(now_ts)

        doc = {
            "space": {
                "name": space['name'],
                "updated_ts": now_ts
            }
        }

        _index = "emp"
        _type = "rooms"

        if ('data' in record):
            #Initialize clients
            rekog_client = boto3.client('rekognition')

            rekog_max_labels = config["rekog_max_labels"]
            rekog_min_conf = float(config["rekog_min_conf"])
            label_watch_list = config["label_watch_list"]

            data = record['data']
            img_data = data[data.find(",") + 1:]
            img_bytes = bytearray(base64.b64decode(img_data))

            rekog_response = rekog_client.detect_labels(
                Image={
                    'Bytes': img_bytes
                },
                MaxLabels=rekog_max_labels,
                MinConfidence=rekog_min_conf
            )
            print('received labels from recognition')
            spaceId = ''
            if ('spaceId' in record):
                spaceId = record['spaceId']
            else:
                r = requests.post('http://development.6awinxwfj9.us-east-1.elasticbeanstalk.com',
                data={'calendar_name' : space['name']})
                if r.status_code != 200:
                    print('Failed to create calendar: ', r.text)
                    response['statusCode'] = 500
                    response['body'] = {"error": "failed to create calendar"}
                    return response
                calendar_id = r.json()['calendarId']
                print('Calendar id', calendar_id)
                spaceId = calendar_id

            doc['space']['image'] = data,
            doc['space']['attributes'] = [r['Name'] for r in rekog_response['Labels']]
            doc['space']['added_ts'] = now_ts
            print('before call: ', doc)
            es_response = es.index(index=_index, id=spaceId, doc_type=_type, body=doc)
            print('after call: ', es_response)
            if es_response['_shards']['failed'] != 0:
                print('Result from ES insert: ', es_response.json())
                response['statusCode'] = 500
                response['body'] = {"error": "failed to add space"}
                return response
            print('done!')
            response['body'] = json.dumps({
                'status': 'created',
                'spaceId': spaceId
            })
        else:
            spaceId = record['spaceId']
            doc['space'] = space

            es_response = es.update(index=_index, id=spaceId, doc_type=_type, body={"doc": doc})
            if es_response['_shards']['failed'] != 0:
                print('Result from ES insert: ', es_response.json())
                response['statusCode'] = 500
                response['body'] = {"error": "failed to edit space"}
                return response
            response['body'] = json.dumps({
                'status': 'updated',
                'spaceId': spaceId
            })
    except Exception:
        response['statusCode'] = 500
        if ('data' in record):
            response['body'] = {"error": "failed to create space"}
        else:
            response['body'] = {"error": "failed to update space"}
    print('Response: ', response)
    return response
