from assets import init
from dotenv import load_dotenv, find_dotenv
from flask import Flask
from flask_caching import Cache
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from ignite import utils 
from os import getenv, environ





app = Flask(__name__)
limiter = Limiter(
    app, 
    key_func=get_remote_address,
    default_limits=['200 per day', '50 per hour']
)
app.config["CACHE_TYPE"] = "simple"
app.config["CACHE_DEFAULT_TIMEOUT"] = 1
init(app)

cache = Cache(app)

# Load our many route files
from ignite import api_routes, basic_routes, fb_routes
