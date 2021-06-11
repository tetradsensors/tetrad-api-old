from tetrad import app
from flask import render_template
# import firebase_admin
# import pyrebase
import json
from functools import wraps
# from firebase_admin import credentials, auth
from flask import Flask, request
import google.cloud.logging 
gcloud_logging_client = google.cloud.logging.Client()
gcloud_logging_client.get_default_handler()
gcloud_logging_client.setup_logging()
import logging
logging.error("Inside basic_routes.py")

# Firebase token authentication for api routes:
# https://medium.com/@nschairer/flask-api-authentication-with-firebase-9affc7b64715
# -- Firebase testing

#Connect to firebase
# cred = credentials.Certificate('ignite/fbAdminConfig.json')
# firebase = firebase_admin.initialize_app(cred)
# pb = pyrebase.initialize_app(json.load(open('ignite/fbconfig.json')))
# #Data source
# users = [{'uid': 1, 'name': 'Noah Schairer'}]

# def check_token(f):
#     @wraps(f)
#     def wrap(*args,**kwargs):
#         if not request.headers.get('authorization'):
#             return {'message': 'No token provided'},400
#         try:
#             user = auth.verify_id_token(request.headers['authorization'])
#             request.user = user
#         except:
#             return {'message':'Invalid token provided.'},400
#         return f(*args, **kwargs)
#     return wrap

# #Api route to get users
# @app.route('/api/userinfo')
# @check_token
# def userinfo():
#     return {'data': users}, 200

# @app.route('/api/signup')
# def signup():
#     email = request.form.get('email')
#     password = request.form.get('password')
#     if email is None or password is None:
#         return {'message': 'Error missing email or password'},400
#     try:
#         user = auth.create_user(
#                email=email,
#                password=password
#         )
#         return {'message': f'Successfully created user {user.uid}'},200
#     except:
#         return {'message': 'Error creating user'},400#Api route to get a new token for a valid user

# @app.route('/api/token')
# def token():
#     email = request.form.get('email')
#     password = request.form.get('password')
#     try:
#         user = pb.auth().sign_in_with_email_and_password(email, password)
#         jwt = user['idToken']
#         return {'token': jwt}, 200
#     except:
#         return {'message': 'There was an error logging in'},400

# -- End Firebase testing

@app.route("/", methods=["GET"])
def index():
    return render_template('blank.html')
    # return render_template('main.html')


# @app.route("/team", methods=["GET"])
# def team():
#     return render_template('team.html')


# @app.route("/request_sensor", methods=["GET"])
# def request_sensor():
#     return render_template('request_sensor.html')


# @app.route("/airu_sensor", methods=["GET"])
# def airu_sensor():
#     return render_template('airu_sensor.html')


# @app.route("/project", methods=["GET"])
# def project():
#     return render_template('project.html')


# @app.route("/newsroom", methods=["GET"])
# def newsroom():
#     return render_template('newsroom.html')


# @app.route("/mailinglist", methods=["GET"])
# def mailinglist():
#     return render_template('mailinglist.html')


# @app.route("/sensor_FAQ", methods=["GET"])
# def sensor_FAQ():
#     return render_template('sensor_FAQ.html')


# @app.route("/about", methods=["GET"])
# def about():
#     return render_template('about.html')
