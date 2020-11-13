from assets import init
# import config
from os import getenv, environ
from dotenv import load_dotenv
from flask import Flask
from flask_caching import Cache
from ignite import utils 

# load_dotenv()
PROJECT_ID = getenv("PROJECTID")

app = Flask(__name__)
# app.config.from_object(config)
app.config["CACHE_TYPE"] = "simple"
app.config["CACHE_DEFAULT_TIMEOUT"] = 1
init(app)

cache = Cache(app)

from ignite import api_routes, basic_routes
