from assets import init
import config
from os import getenv, environ
from dotenv import load_dotenv
from flask import Flask
from flask_caching import Cache
from google.cloud.bigquery import Client

load_dotenv()
PROJECT_ID = getenv("PROJECTID")

app = Flask(__name__)
app.config.from_object(config)
app.config["CACHE_TYPE"] = "simple"
app.config["CACHE_DEFAULT_TIMEOUT"] = 1
init(app)

cache = Cache(app)

environ["GOOGLE_APPLICATION_CREDENTIALS"] = getenv("GOOGLE_APPLICATION_CREDENTIALS_PATH")
bq_client = Client(project=PROJECT_ID)

from ignite import utils
elevation_interpolator = utils.setupElevationInterpolator('elevation_map.mat')

from ignite import api_routes, basic_routes
