# Code derived from:
# https://medium.com/@nschairer/flask-api-authentication-with-firebase-9affc7b64715
import json 
import requests 
import functools
from firebase_admin import auth
from google.cloud import firestore, secretmanager
from os import getenv
from flask import request
import re
import logging


fs_client = firestore.Client()


def fs_get_in_group(uid, group):
    if isinstance(group, str):
        print("fs_get_in_group", "was str")
        doc = fs_client.collection(getenv('FS_USER_GROUPS_COLLECTION')).document(f'{group}').get()
        return (doc.exists) and (f'{uid}' in list(doc.get(getenv('FS_USER_GROUPS_UIDS_KEY'))))
    elif isinstance(group, list):
        print("fs_get_in_group", "was list")
        docs = fs_client.collection(getenv('FS_USER_GROUPS_COLLECTION')).where('__name__', 'in', group).stream()
        valid_uids = []
        for doc in docs:
            print('doc.get("uid"):', doc.get(getenv('FS_USER_GROUPS_UIDS_KEY')))
            valid_uids += list(doc.get(getenv('FS_USER_GROUPS_UIDS_KEY'))) 
        print('valid_uids:', valid_uids)
        return (f'{uid}' in valid_uids)
    else:
        print("fs_get_in_group", "was None")
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
    Decorator for api routes. Check if user filled 'authorization' header field
    then make sure they are in the 'admin' group in Firestore. 
    """
    def inner(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            if not request.headers.get('authorization'):
                return {'message': 'No token provided. Please provide a key/value pair for header: authorization:<session JWT>"'}, 400
            try:
                user = auth.verify_id_token(request.headers['authorization'])
                request.user = user
            except Exception as e:
                return {'message':'Invalid token provided.' + str(e)}, 400

            if not fs_get_in_group(user['user_id'], group_or_groups):
                return {'message': 'User does not have appropriate permissions to use this route.'}, 400
            
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
    name = f"projects/{getenv('PROJECT_ID')}/secrets/{secret_id}/versions/{version_id}"

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