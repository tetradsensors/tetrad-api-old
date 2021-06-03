DEBUG = False
import os

if DEBUG:
    print('Running in DEBUG mode')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/tombo/Tetrad/global/tetrad.json'
    from dotenv import load_dotenv
    load_dotenv()

from assets import init
from flask import Flask
from flask_caching import Cache
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_cors import CORS, cross_origin
from firebase_admin import initialize_app
from tetrad import utils
from google.cloud import bigquery

if DEBUG:
    app = Flask(__name__)
else:
    import google.cloud.logging 
    gcloud_logging_client = google.cloud.logging.Client()
    gcloud_logging_client.get_default_handler()
    gcloud_logging_client.setup_logging()
    import logging

    # app = Flask(__name__, subdomain_matching=True)
    app = Flask(__name__)

    # cors = CORS(app, resources={r"/request_historical*": {"origins": "*"}})
    cors = CORS(app)
    #Below Route to be tested once we get the API running
    #cors = CORS(app, ressources={r"/request_historical*": {"origins": "https://www.tetradsensors.com/*"}})
    
    # app.config['SERVER_NAME'] = os.getenv('DOMAIN_NAME')
    
    app.config["CACHE_TYPE"] = "simple"
    app.config["CACHE_DEFAULT_TIMEOUT"] = 1
    app.config['CORS_HEADERS'] = 'Content-Type'

    limiter = Limiter(
        app, 
        key_func=get_remote_address,
        default_limits=['5000 per day']
    )

    cache = Cache(app)

init(app)

firebase_app = initialize_app()

from tetrad import utils

bq_client = bigquery.Client()

# WARNING - current status of the elevation_map.mat files is that longitude is the first coordinate
elevation_interpolator = utils.setupElevationInterpolator('elevation_map.mat')

# Load our many route files
from tetrad import api_routes, fb_routes, ota_routes
