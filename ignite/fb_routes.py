from firebase_admin import auth, initialize_app
from flask import request
from ignite import app, fb_utils


firebase_app = initialize_app()


@app.route("/api/signup")
def signup():
    email = request.form.get('email')
    password = request.form.get('password')
    if email is None or password is None:
        return {'message': 'Error missing email or password'}, 400
    try:
        user = auth.create_user(
               email=email,
               password=password
        )
        return {'message': f'Successfully created user {user.uid}'}, 200
    except Exception as e:
        return {'message': str(e)}, 400


@app.route("/api/requestToken", methods=["GET"])
def requestToken():
    email = request.form.get('email')
    password = request.form.get('password')
    try:
        user = fb_utils.sign_in_with_email_and_password(email, password)
        jwt = user['idToken']
        return {'token': jwt}, 200
    except:
        return {'message': 'There was an error logging in.'}, 400


@app.route("/api/requestUid", methods=["GET"])
def requestUid():
    email = request.form.get('email')
    password = request.form.get('password')
    try:
        user = fb_utils.sign_in_with_email_and_password(email, password)
    except Exception as e:
        return {'message': str(e)}, 400
    if user['registered']:
        return {'user_id': user['localId']}, 200
    else:
        return {'message': 'Error'}