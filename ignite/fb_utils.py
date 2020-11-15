# Code derived from:
# https://medium.com/@nschairer/flask-api-authentication-with-firebase-9affc7b64715
import json 
import requests 
import functools
from firebase_admin import auth
from google.cloud import firestore, secretmanager
from os import getenv
from flask import request


fs_client = firestore.Client()


def fs_get_in_group(uid, group):
    doc = fs_client.collection('user_groups').document(f'{group}').get()
    return (doc.exists) and (f'{uid}' in list(doc.get('uid')))


def check_token(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not request.headers.get('authorization'):
            return {'message': 'No token provided. Please provide a key/value pair for header: authorization:<session JWT>"'}, 400
        try:
            user = auth.verify_id_token(request.headers['authorization'])
            request.user = user
        except Exception as e:
            return {'message':'Invalid token provided.' + str(e)}, 400
        return f(*args, **kwargs)
    return wrapper


def admin(f):
    """
    Decorator for api routes. Check if user filled 'authorization' header field
    then make sure they are in the 'admin' group in Firestore. 
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not request.headers.get('authorization'):
            return {'message': 'No token provided. Please provide a key/value pair for header: authorization:<session JWT>"'}, 400
        try:
            user = auth.verify_id_token(request.headers['authorization'])
            request.user = user
        except Exception as e:
            return {'message':'Invalid token provided.' + str(e)}, 400

        if not fs_get_in_group(user['user_id'], 'admin'):
            return {'message': 'User does not have appropriate permissions to use this route.'}, 400
        return f(*args, **kwargs)
    return wrapper


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

    return json.loads(response.payload.data.decode("UTF-8"))


def sign_in_with_email_and_password(email, password):
    secret = _access_secret_version(getenv("FB_CONFIG_SECRET"))
    request_ref = f"https://www.googleapis.com/identitytoolkit/v3/relyingparty/verifyPassword?key={secret['apiKey']}"
    headers = {"content-type": "application/json; charset=UTF-8"}
    data = json.dumps({"email": email, "password": password, "returnSecureToken": True})

    request_object = requests.post(request_ref, headers=headers, data=data)
    return request_object.json()