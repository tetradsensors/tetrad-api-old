import assets
# import config
import os
from dotenv import load_dotenv
from flask import Flask
from flask_caching import Cache
from google.cloud import bigquery
import time
import sys
import json


load_dotenv()
PROJECT_ID = os.getenv("PROJECTID")


# logfile = "serve.log"
# logging.basicConfig(filename=logfile, level=logging.DEBUG, format = '%(levelname)s: %(filename)s: %(message)s')
# logging.basicConfig(stream=sys.stdout, level=logging.INFO, format = '%(levelname)s: %(filename)s: %(message)s')
# logging.info('API server started at %s', time.asctime(time.localtime()))
import google.cloud.logging 
log_client = google.cloud.logging.Client()
log_client.get_default_handler()
log_client.setup_logging()

app = Flask(__name__)
# app.config.from_object(config)
app.config["CACHE_TYPE"] = "simple"
app.config["CACHE_DEFAULT_TIMEOUT"] = 1
cache = Cache(app)
assets.init(app)

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/tombo/Tetrad/global/tetrad.json"
bq_client = bigquery.Client(project=PROJECT_ID)

from tetrad import jsonutils
# WARNING - current status of the elevation_map.mat files is that longitude is the first coordinate
#elevation_interpolator = utils.setupElevationInterpolator('elevation_map.mat')
with open('area_params.json') as json_file:
        json_temp = json.load(json_file)
_area_models = jsonutils.buildAreaModelsFromJson(json_temp)

from tetrad import api_routes
