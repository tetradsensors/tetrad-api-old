from firebase_admin import auth#, initialize_app
from flask import request, send_file
from tetrad import app, admin_utils
import functools
import traceback 
from os import getenv
from io import BytesIO, TextIOWrapper
import datetime 
import json 
import csv 
import traceback
from hashlib import md5
from werkzeug.utils import secure_filename
from google.cloud import bigquery
import google.cloud.logging 
gcloud_logging_client = google.cloud.logging.Client()
gcloud_logging_client.get_default_handler()
gcloud_logging_client.setup_logging()
import logging
logging.error("Inside iot_routes.py")

# bq_client = bigquery.Client()

# def _handle_error(exception):
#     message = 'Exception: %s Error traceback: %s' % (str(exception), traceback.format_exc())
#     logging.error(message)
#     return message

# def get_telemetry_file_info(filename):
#     '''
#     Filename should be:
#     <MAC Address>_YYYY-MM-DD.csv
#     '''
#     logging.error('filename')
#     logging.error(filename)
#     if '.' not in filename: 
#         logging.error("no '.' in filename")
#         return None 
#     if filename.rsplit('.')[1].lower() != 'csv': 
#         logging.error("no csv extension")
#         return None
#     deviceID = filename.split('_')[0]
#     logging.error("deviceID = " + deviceID)
#     if len(deviceID) != 12:
#         logging.error("deviceID != 12")
#         return None 
#     try:
#         dt = datetime.datetime.strptime(filename.split('_')[1].split('.')[0], '%Y-%m-%d')
#         logging.error(dt)
#         return deviceID, dt
#     except Exception as e:
#         _handle_error(e)
#         return None 


@app.route("/dnl", methods=["GET"], subdomain=getenv('SUBDOMAIN_OTA'))
@admin_utils.check_creds(uid=getenv('FB_AIRU_UID'))
def dnl():
    """
    Download the blob from Google Storage
    """
    logging.error("HTTP_HOST: " + request.environ.get('HTTP_HOST'))
    if not request.args.get('filename'):
        return "Must specify a filename argument.", 418
    filename = request.args.get('filename')
    binary = admin_utils.gs_get_blob(getenv('GS_BUCKET_OTA'), filename, dnl_type="bytes")
    if binary == 404:
        return "File does not exist", 404
    if isinstance(binary, bytes):
        binary = BytesIO(binary)
        return send_file(binary, mimetype='application/octet-stream')
    else:
        return "Bad type", 418


# @app.route("/offline", methods=["POST"], subdomain=getenv("SUBDOMAIN_OTA"))
# # @admin_utils.check_creds(uid=getenv('PB_AIRU_UID'))
# def offline():
#     '''
#     AirU devices upload telemetry files from their SD card.  
#     They do this with data collected while the device was offline. 
#     '''

#     TABLE_REF = bq_client.get_table(getenv('BIGQUERY_TABLE_DEV'))

#     if 'file' not in request.files:
#         return "No file part", 404
    
#     f_csv = request.files['file']
    
#     if f_csv.filename == '':
#         return "No selected file", 404

#     filename = secure_filename(f_csv.filename)
    
#     try:
#         deviceID, date = get_telemetry_file_info(filename)
#     except:
#         return "Bad filename", 404
    
#     rows, row_ids = [], []
#     try:
#         with TextIOWrapper(f_csv, encoding='utf-8') as f_text:
#             for row in csv.DictReader(f_text):
#                 row[getenv('BQ_FIELD_DEVICEID')] = deviceID
#                 row[getenv('BQ_FIELD_MICS_HEATER')] = 'true' if row[getenv('BQ_FIELD_MICS_HEATER')] == '1' else 'false'
#                 rows.append(row)
#                 row_ids.append(
#                     md5(
#                         (str(row[getenv('BQ_FIELD_TIMESTAMP')]) + str(row[getenv('BQ_FIELD_DEVICEID')])).encode()
#                     ).hexdigest()
#                 )
#             logging.error('rows')
#             logging.error(rows)
#             logging.error('row_ids')
#             logging.error(row_ids)
    
#     except Exception as e:
#         msg = _handle_error(e)
#         return msg, 404

#     r = bq_client.insert_rows(TABLE_REF, rows=rows, row_ids=row_ids)
#     if r != []:
#         logging.error("errors")
#         logging.error(r)
#         return "Insert errors", 404
#     else:
#         logging.error("clean upload")
#         return "Clean upload", 200

    
    

