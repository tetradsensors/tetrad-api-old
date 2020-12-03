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
import logging 


fs_client = firestore.Client()


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


# def check_token(f):
#     @functools.wraps(f)
#     def wrapper(*args, **kwargs):
#         if not request.headers.get('authorization'):
#             return {'message': 'No token provided. Please provide a key/value pair for header: authorization:<session JWT>"'}, 400
#         try:
#             user = auth.verify_id_token(request.headers['authorization'])
#             request.user = user
#         except Exception as e:
#             return {'message':'Invalid token provided.' + str(e)}, 400
#         return f(*args, **kwargs)
#     return wrapper


# def admin(f):
#     """
#     Decorator for api routes. Check if user filled 'authorization' header field
#     then make sure they are in the 'admin' group in Firestore. 
#     """
#     @functools.wraps(f)
#     def wrapper(*args, **kwargs):
#         if not request.headers.get('authorization'):
#             return {'message': 'No token provided. Please provide a key/value pair for header: authorization:<session JWT>"'}, 400
#         try:
#             user = auth.verify_id_token(request.headers['authorization'])
#             request.user = user
#         except Exception as e:
#             return {'message':'Invalid token provided.' + str(e)}, 400

#         if not fs_get_in_group(user['user_id'], 'admin'):
#             return {'message': 'User does not have appropriate permissions to use this route.'}, 400
#         return f(*args, **kwargs)
#     return wrapper

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


def sign_in_with_email_and_password(email, password):
    secret = _access_secret_version(getenv("FB_CONFIG_SECRET"))
    url = f"https://www.googleapis.com/identitytoolkit/v3/relyingparty/verifyPassword?key={secret['apiKey']}"
    headers = {"content-type": "application/json; charset=UTF-8"}
    data = json.dumps({"email": email, "password": password, "returnSecureToken": True})
    request_object = requests.post(url, headers=headers, data=data)
    return request_object.json()


def check_email(email):
    """
    Email must be this format:
        <2,>@<2,3>.<2,3>
        ex: aa@aa.aa - GOOD
            a@aa.aa  - BAD
            aa@a.aa  - BAD
            aa@aa.a  - BAD 
    """
    # REGEX explanation:
    # http://rumkin.com/software/email/rules.php
    #   Email addresses are <localpart>@<domain>
    #   localpart can be any ASCII from 0x21-0x7F
    #   localpart cannot start or end with .
    #   localpart cannot contain two dots: ..
    #   domain can only contain letters and numbers
    #   domain can be alphanumerics separated by dots
    #   domain cannot start or end with .
    #   domain cannot contain two dots: ..
    regex = r'^(?!\.)(?:(?!\.\.)[\x21-\x7F])+(?:(?!\.)[\x21-\x7F])[@][\w]+([\w]+[.]?)*[.]\w{1,3}$'
    if re.search(regex, email):
        return True
    else:
        return False


def check_password(password):
    """
    Password must be at least 8 characters and include lower, upper, number, and one of: @$!#%*?&
        OR
    Password must be at least 20 characters and include lower, upper, number. 
    """
    regex = r'^(^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*#?&])[A-Za-z\d@$!#%*?&]{8,}$)|(^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)[A-Za-z\d]{20,}$)'
    if re.search(regex, password): 
        return True 
    else: 
        return False


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
        return 404
    else:
        return None