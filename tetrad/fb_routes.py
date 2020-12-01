from firebase_admin import auth, initialize_app
from flask import request
from tetrad import app, fb_utils
import traceback 
import logging 


firebase_app = initialize_app()


@app.route("/api/signup", methods=["POST"])
def signup():
    # logging.error("Inside signup")
    # logging.error(request.args)
    # logging.error(request.form)
    email = request.form.get('email')
    password = request.form.get('password')
    if email is None or password is None:
        # logging.warning('email or password is None')
        return 'ERROR: Missing email or password', 400
    if not fb_utils.check_email(email):
        # logging.warning(f'email: [{email}] no good.')
        return 'ERROR: Invalid email.', 400
    if not fb_utils.check_password(password):
        # logging.warning(f'password: [{password}] no good.')
        return 'ERROR: Invalid password. Password must be at least 8 characters and include: [a-z], [A-Z], [0-9], [@$!#%*?&]', 400
    try:
        user = auth.create_user(
               email=email,
               password=password
        )
        # logging.debug(vars(user))
        # logging.debug(user)
        return {'message': f'Successfully created user {user.uid}'}, 200
    except Exception as e:
        logging.error(traceback.print_exc())
        return str(repr(e)), 400


@app.route("/api/requestToken", methods=["POST"])
def requestToken():
    email = request.form.get('email')
    password = request.form.get('password')
    try:
        user = fb_utils.sign_in_with_email_and_password(email, password)
        jwt = user['idToken']
        return {'token': jwt}, 200
    except Exception as e:
        logging.error(traceback.print_exc())
        return 'ERROR: There was an error logging in:' + repr(e), 400


@app.route("/api/requestUid", methods=["POST"])
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