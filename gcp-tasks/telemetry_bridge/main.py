'''
This Cloud function is responsible for:
- Ingesting temeletry messages from AirU devices
- forwarding messages to the correct BigQuery tables
'''
import base64
import json
import logging
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

def ps_bq_bridge(event, context):
    if 'data' in event:
        try:
            _insert_into_bigquery(event, context)
        except Exception:
            _handle_error(event['deviceId'])


def _insert_into_bigquery(event, context):
    data = base64.b64decode(event['data']).decode('utf-8')
    
    deviceId = event['attributes']['deviceId'][1:]
    
    row = json.loads(data)
     
    row['DeviceID'] = deviceId

    # Uploads from SD card send a timestamp, normal messages do not
    if 'Timestamp' not in row:
        row['Timestamp'] = context.timestamp

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
                                 retry=retry.Retry(deadline=30))
    if errors != []:
        raise BigQueryError(errors)


def _handle_success(deviceID):
    message = 'Device \'%s\' streamed into BigQuery' % deviceID
    logging.info(message)


def _handle_error(deviceID):
    message = 'Error streaming from device \'%s\'. Cause: %s' % (deviceID, traceback.format_exc())
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