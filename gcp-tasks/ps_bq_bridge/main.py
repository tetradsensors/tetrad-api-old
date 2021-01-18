'''
This Cloud function is responsible for:
- Ingesting temeletry messages from AirU devices
- forwarding messages to the correct BigQuery tables
'''
import base64
import json
from os import getenv
import logging
import traceback
from datetime import datetime
from google.cloud import bigquery
from google.api_core import retry
import pytz

bq = bigquery.Client()

METRIC_ERROR_MAP = {
    getenv("FIELD_ELE"):    0,
    getenv("FIELD_PM1"):    -1,
    getenv("FIELD_PM2"):    -1,
    getenv("FIELD_PM10"):   -1,
    getenv("FIELD_TEMP"):   -1000,
    getenv("FIELD_HUM"):    -1000,
    getenv("FIELD_RED"):    10000,
    getenv("FIELD_NOX"):    10000,
}

# http://www.eecs.umich.edu/courses/eecs380/HANDOUTS/PROJ2/InsidePoly.html
def inPoly(p, poly):
    """
    NOTE Polygon can't cross Anti-Meridian
    @param p: Point as (Lat, Lon)
    @param poly: list of (Lat,Lon) points. Neighboring polygon vertices indicate lines
    @return: True if in poly, False otherwise
    """
    c = False
    pp = list(poly)
    N = len(poly)
    for i in range(N):
        j = (i - 1) % N
        if ((((pp[i][0] <= p[0]) and (p[0] < pp[j][0])) or
             ((pp[j][0] <= p[0]) and (p[0] < pp[i][0]))) and
            (p[1] < (pp[j][1] - pp[i][1]) * (p[0] - pp[i][0]) / (pp[j][0] - pp[i][0]) + pp[i][1])):
            c = not c
    return c


def pointToTableName(p):
    if not sum(p):
        return getenv('BIGQUERY_TABLE_BADGPS')
    polys = json.load(open(getenv('BOUNDING_POLYS_FILENAME')))
    for table_name, poly in polys.items():
        if inPoly(p, poly):
            return table_name
    return getenv('BIGQUERY_TABLE_GLOBAL')


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
     
    row[getenv("FIELD_ID")] = deviceId

    # Uploads from SD card send a timestamp, normal messages do not
    if getenv("FIELD_TS") not in row:
        row[getenv("FIELD_TS")] = context.timestamp

    # Replace error codes with None - blank in BigQuery
    for k in row:
        if k in METRIC_ERROR_MAP:
            if row[k] == METRIC_ERROR_MAP[k]:
                row[k] = None 
                if k == getenv("FIELD_NOX"):
                    row[getenv("FIELD_HTR")] = None
                    
    table_name = pointToTableName((row[getenv("FIELD_LAT")], row[getenv("FIELD_LON")]))
    logging.info("Table: " + table_name)
    table = bq.dataset(getenv('BIGQUERY_DATASET')).table(table_name)
    errors = bq.insert_rows_json(table,
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