from firebase_admin import auth#, initialize_app
from flask import request, send_file
from tetrad import app, admin_utils
import traceback 
from os import getenv
from io import BytesIO
import google.cloud.logging
gcloud_logging_client = google.cloud.logging.Client()
gcloud_logging_client.get_default_handler()
gcloud_logging_client.setup_logging()
import logging


# firebase_app = initialize_app()


@app.route("/signup", methods=["POST"], subdomain=getenv('SUBDOMAIN_API'))
# @app.route("/signup", methods=["POST"])
def signup():
    logging.error("HTTP_HOST: " + request.environ.get('HTTP_HOST'))
    logging.error("SERVER_NAME: " + app.config['SERVER_NAME'])
    email = request.form.get('email')
    password = request.form.get('password')
    if email is None or password is None:
        return 'ERROR: Missing email or password', 400
    if not admin_utils.check_email(email):
        return 'ERROR: Invalid email.', 400
    if not admin_utils.check_password(password):
        return 'ERROR: Invalid password. Password must be at least 8 characters and include: [a-z], [A-Z], [0-9], [@$!#%*?&]', 400
    try:
        user = auth.create_user(
               email=email,
               password=password
        )
        return {'message': f'Successfully created user {user.uid}'}, 200
    except Exception as e:
        logging.error(traceback.print_exc())
        return str(repr(e)), 400


@app.route("/requestToken", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
# @app.route("/requestToken", methods=["GET"])
def requestToken():
    email = request.form.get('email')
    password = request.form.get('password')
    try:
        user = admin_utils.sign_in_with_email_and_password(email, password)
        jwt = user['idToken']
        return {'token': jwt}, 200
    except Exception as e:
        logging.error(traceback.print_exc())
        return 'ERROR: There was an error logging in:' + repr(e), 400


@app.route("/requestUid", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
# @app.route("/api/requestUid", methods=["GET"])
def requestUid():
    email = request.form.get('email')
    password = request.form.get('password')
    try:
        user = admin_utils.sign_in_with_email_and_password(email, password)
    except Exception as e:
        return {'message': str(e)}, 400
    if user['registered']:
        return {'user_id': user['localId']}, 200
    else:
        return {'message': 'Error'}


