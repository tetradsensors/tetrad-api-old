from firebase_admin import auth, initialize_app
from flask import request, send_file
from tetrad import app, fb_utils
import traceback 
from os import getenv
from io import BytesIO
import logging 

firebase_app = initialize_app()


@app.route("/api/signup", methods=["POST"])
def signup():
    email = request.form.get('email')
    password = request.form.get('password')
    if email is None or password is None:
        return 'ERROR: Missing email or password', 400
    if not fb_utils.check_email(email):
        return 'ERROR: Invalid email.', 400
    if not fb_utils.check_password(password):
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


@app.route("/api/ota", methods=["POST"])
@fb_utils.ingroup('airuv2')
def ota():
    """
    Download the blob 
    """
    binary_name = str(request.args.get('name'))
    if not binary_name.endswith('.bin'):
        return "Bad name", 418
    binary = fb_utils.gs_get_blob(getenv('GS_BUCKET_OTA'), binary_name, dnl_type="bytes")
    if binary == 404:
        return "File does not exist", 404
    if isinstance(binary, bytes):
        binary = BytesIO(binary)
        return send_file(binary, mimetype='application/octet-stream')
    else:
        return "Bad type", 418
