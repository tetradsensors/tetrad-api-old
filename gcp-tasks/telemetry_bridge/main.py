'''
This Cloud function is responsible for:
- Ingesting temeletry messages from AirU devices
- forwarding messages to the correct BigQuery tables
'''
import base64
import json
import logging
from hashlib import md5
import traceback
from datetime import datetime
from google.cloud import bigquery
from google.api_core import retry
import pytz


BQ_DATASET = "telemetry"
BQ_TABLE = "dev"
BQ = bigquery.Client()

METRIC_ERROR_MAP = {
    'Latitude':     0,
    'Longitude':    0,
    'Elevation':    0,
    'PM1':          -1,
    'PM2_5':        -1,
    'PM10':         -1,
    'Temperature':  -1000,
    'Humidity':     -1000,
    'MicsRED':      10000,
    'MicsNOX':      10000,
}

REQUIRED = ['Timestamp', 'DeviceID']


def ps_bq_bridge(event, context):
    if 'data' in event:
        try:
            _insert_into_bigquery(event, context)
        except Exception:
            _handle_error(event)


def _insert_into_bigquery(event, context):
    data = base64.b64decode(event['data']).decode('utf-8')

    try:
        with open('local.txt') as f:
            print('local file read:', f.read())
    except:
        _handle_error(event)
    
    try:
        deviceId = event['attributes']['deviceId'][1:]
    except:
        _handle_error(event)
    
    row = json.loads(data)

    for req in REQUIRED:
        try:
            assert(req in row)
        except:
            _handle_error(event)
            
    try:
        assert(deviceId == row['DeviceID'])
    except:
        _handle_error(event)

    try:
        assert(row['Timestamp'] > 1500000000)
    except:
        _handle_error(event)

    row_ids = [md5((str(row['Timestamp']) + str(row['DeviceID'])).encode()).hexdigest()]

    # Replace error codes with None - blank in BigQuery
    for k in row:
        if k in METRIC_ERROR_MAP:
            if row[k] == METRIC_ERROR_MAP[k]:
                row[k] = None 
                if k == 'MicsNOX':
                    row['MicsHeater'] = None
                    
    table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
    errors = BQ.insert_rows_json(table,
                                 json_rows=[row],
                                 row_ids=row_ids,
                                 retry=retry.Retry(deadline=30))
    if errors != []:
        raise BigQueryError(errors)


def _handle_success(deviceID):
    message = 'Device \'%s\' streamed into BigQuery' % deviceID
    logging.info(message)


def _handle_error(event):
    if 'deviceId' in event['attributes']:
        deviceId = event['attributes']['deviceId']
    else:
        deviceId = 'NO DEVICE ID'
    message = 'Error streaming from device \'%s\'. Cause: %s' % (deviceId, traceback.format_exc())
    logging.error(message)


class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened''' 

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)