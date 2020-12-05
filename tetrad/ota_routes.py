from firebase_admin import auth#, initialize_app
from flask import request, send_file
from tetrad import app, admin_utils
import functools
import traceback 
from os import getenv
from io import BytesIO
import logging 

# firebase_app = initialize_app()
subdomain = f"{getenv('OTA_SUBDOMAIN')}.{getenv('DOMAIN_NAME')}"


@app.route("/ota/<string:filename>", methods=["GET"], subdomain=subdomain)
@admin_utils.check_creds
def ota(filename):
    """
    Download the blob from Google Storage
    """
    binary = admin_utils.gs_get_blob(getenv('GS_BUCKET_OTA'), filename, dnl_type="bytes")
    if binary == 404:
        return "File does not exist", 404
    if isinstance(binary, bytes):
        binary = BytesIO(binary)
        return send_file(binary, mimetype='application/octet-stream')
    else:
        return "Bad type", 418
