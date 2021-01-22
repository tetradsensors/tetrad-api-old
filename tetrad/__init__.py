from assets import init
from flask import Flask
from flask_caching import Cache
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_cors import CORS, cross_origin
from firebase_admin import initialize_app
from tetrad import utils 
from os import getenv, environ
import google.cloud.logging 
gcloud_logging_client = google.cloud.logging.Client()
gcloud_logging_client.get_default_handler()
gcloud_logging_client.setup_logging()
import logging

logging.error("Inside __init__.py setup")

# app = Flask(__name__)
app = Flask(__name__, subdomain_matching=True)
# cors = CORS(app, resources={r"/request_historical*": {"origins": "*"}})
cors = CORS(app)
#Below Route to be tested once we get the API running
#cors = CORS(app, ressources={r"/request_historical*": {"origins": "https://www.tetradsensors.com/*"}})
app.config['SERVER_NAME'] = getenv('DOMAIN_NAME')
app.config["CACHE_TYPE"] = "simple"
app.config["CACHE_DEFAULT_TIMEOUT"] = 1
app.config['CORS_HEADERS'] = 'Content-Type'

init(app)

limiter = Limiter(
    app, 
    key_func=get_remote_address,
    default_limits=['2000 per day']
)

firebase_app = initialize_app()

cache = Cache(app)

# Load our many route files
from tetrad import api_routes, basic_routes, fb_routes, ota_routes
