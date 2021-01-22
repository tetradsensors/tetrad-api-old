from firebase_admin import auth#, initialize_app
from flask import request, send_file
from tetrad import app, admin_utils
import functools
import traceback 
from os import getenv
from io import BytesIO
import google.cloud.logging 
gcloud_logging_client = google.cloud.logging.Client()
gcloud_logging_client.get_default_handler()
gcloud_logging_client.setup_logging()
import logging


# firebase_app = initialize_app()


@app.route("/dnl", methods=["GET"], subdomain=getenv('SUBDOMAIN_OTA'))
# @app.route("/dnl", methods=["GET"])
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
