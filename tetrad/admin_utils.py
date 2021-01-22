# Code derived from:
# https://medium.com/@nschairer/flask-api-authentication-with-firebase-9affc7b64715
import json 
import requests 
import functools
from firebase_admin import auth
from google.cloud import firestore, secretmanager, storage
from os import getenv
from flask import request
import re
import base64
import google.cloud.logging 
gcloud_logging_client = google.cloud.logging.Client()
gcloud_logging_client.get_default_handler()
gcloud_logging_client.setup_logging()
import logging



fs_client = firestore.Client()


def check_creds(uid):
    """ 
    Check that the supplied email/password from header match
    the UID supplied here.
    """
    def inner(f): 
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            
            # Check for header
            if not request.headers.get(getenv('FB_AUTH_HEADER')):
                return f"No {getenv('FB_AUTH_HEADER')} supplied. Header must look like <br><br>{getenv('FB_AUTH_HEADER')}: Basic <email:password><br><br> where <email:password> are Base64 encoded.", 401
            
            # Get email, password
            try:
                userpass = request.headers.get(getenv('FB_AUTH_HEADER')).split('Basic ')[1]
                email, password = base64.b64decode(userpass.encode()).decode().split(':')
            except:
                return f"Bad formatting for {getenv('FB_AUTH_HEADER')} header. Header must look like <br><br>{getenv('FB_AUTH_HEADER')}: Basic <email:password><br><br> where <email:password> are Base64 encoded.", 401
            
            # Check email, password
            if (not check_email(email)) or (not check_password(password)):
                return "Invalid formatting for email or password", 401

            # Sign them in and check the response
            try:
                user = sign_in_with_email_and_password(email, password)
                user_uid = user['localId']
            except Exception as e:
                return "Could not sign in user. <b>ERROR:</b><br><br>" + repr(e), 401

            # Check that the ID matches
            if user_uid != uid:
                return "User is not validated to run this route", 401

            # Sign-in was successful, execute the calling function. 
            return f(*args, **kwargs)
        return wrapper
    return inner


def ingroup(group_or_groups): 
    """
    Decorator for api routes. Check if user filled 'Authentication' header field
    then make sure they are in the given group[s] in Firestore. 
    """
    def inner(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            if not request.headers.get(getenv('FB_AUTH_HEADER')):
                return f'No token provided. Please provide a key/value pair for header: {getenv("FB_AUTH_HEADER")}:<session JWT>"', 401
            
            try:
                user = auth.verify_id_token(request.headers.get(getenv('FB_AUTH_HEADER')))
                request.user = user
            except Exception as e:
                return 'Invalid token provided.' + repr(e), 401

            if not fs_get_in_group(user['user_id'], group_or_groups):
                return 'User does not have appropriate permissions to use this route.', 401
            # finally, execute the calling function
            return f(*args, **kwargs)
        return wrapper
    return inner 


def _access_secret_version(secret_id, version_id="latest"):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{getenv('GOOGLE_CLOUD_PROJECT')}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})
    json_response = json.loads(response.payload.data.decode("UTF-8"))
    return json_response


def fs_get_in_group(uid, group):
    if isinstance(group, str):
        doc = fs_client.collection(getenv('FS_USER_GROUPS_COLLECTION')).document(f'{group}').get()
        return (doc.exists) and (f'{uid}' in list(doc.get(getenv('FS_USER_GROUPS_UIDS_KEY'))))
    elif isinstance(group, list):
        docs = fs_client.collection(getenv('FS_USER_GROUPS_COLLECTION')).where('__name__', 'in', group).stream()
        valid_uids = []
        for doc in docs:
            valid_uids += list(doc.get(getenv('FS_USER_GROUPS_UIDS_KEY'))) 
        return (f'{uid}' in valid_uids)
    else:
        return False


def sign_in_with_email_and_password(email, password):
    secret = _access_secret_version(getenv("FB_CONFIG_SECRET"))
    url = f"https://www.googleapis.com/identitytoolkit/v3/relyingparty/verifyPassword?key={secret['apiKey']}"
    headers = {"content-type": "application/json; charset=UTF-8"}
    data = json.dumps({"email": email, "password": password, "returnSecureToken": True})
    request_object = requests.post(url, headers=headers, data=data)
    return request_object.json()


def check_email(email):
    """
    Check email. Must follow these rules: 
    REGEX explanation:
    http://rumkin.com/software/email/rules.php
      - Email addresses are <localpart>@<domain>
      - localpart can be any alphanumeric or .!#$%&'*+-/=?^`{|}~
      - localpart cannot start or end with .
      - localpart cannot contain two dots: ..
      - domain can only contain letters and numbers
      - domain can be alphanumerics or '-' separated by dots
      - domain cannot start or end with . or -
      - domain cannot contain two dots: ..
      - each domain label is <64
    """
    regex = r"^(?![\w.!#$%&'*+\-\/=?^`{|}~]*\.\.)[^.][\w.!#$%&'*+\-\/=?^`{|}~]*[^.][@]((?!-)[a-zA-Z0-9-]{1,63}(?<!-)\.)+[a-zA-Z]{2,63}$"
    return bool(re.search(regex, email))  


def check_password(password):
    """
    Password must be at least 8 characters and 
    include lower, upper, number, and one of: @$!#%*?&
        OR
    Password must be at least 20 characters and 
    include lower, upper, number. 
    """
    regex = r"^(^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*#?&])[A-Za-z\d@$!#%*?&]{8,}$)|(^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)[A-Za-z\d]{20,}$)"
    return bool(re.search(regex, password))


def gs_get_blob(bucket_name, source_blob_name, dnl_type):
    """
    Download blob from GS bucket into bytes object
    @parm dnl_type: one of "string", "text", "bytes"
    """
    c = storage.Client()
    bucket = c.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    try:
        if dnl_type == "string":
            return blob.download_as_string()
        elif dnl_type == "text":
            return blob.download_as_text()
        elif dnl_type == "bytes":
            return blob.download_as_bytes()
    except Exception as e:
        return repr(e), 404
    else:
        return None