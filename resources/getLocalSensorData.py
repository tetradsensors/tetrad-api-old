from flask_restful import reqparse
from google.cloud import bigquery
from common.params import URL_PARAMS, PARAMS_HELP_MESSAGES, lat_check, lon_check, radius_check_m
from flask_restful import Resource
from flask_restful.reqparse import RequestParser 
from flask_restful.inputs import datetime_from_iso8601
from flask import jsonify
import common.utils
import common.jsonutils
import json

from common.decorators import processPreRequest

arguments = RequestParser()
arguments.add_argument(URL_PARAMS.START_TIME,    type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.START_TIME, required=True)
arguments.add_argument(URL_PARAMS.END_TIME,      type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.END_TIME,   required=True)
arguments.add_argument(URL_PARAMS.LAT,           type=lat_check,             help=PARAMS_HELP_MESSAGES.LAT,        required=True)
arguments.add_argument(URL_PARAMS.LON,           type=lon_check,             help=PARAMS_HELP_MESSAGES.LON,        required=True)
arguments.add_argument(URL_PARAMS.RADIUS,        type=radius_check_m,        help=PARAMS_HELP_MESSAGES.RADIUS,     required=True)

class getLocalSensorData(Resource):

    @processPreRequest
    def get(self, **kwargs):
        args = arguments.parse_args()

        start_datetime = args[URL_PARAMS.START_TIME]
        end_datetime = args[URL_PARAMS.END_TIME]
        lat = args[URL_PARAMS.LAT]
        lon = args[URL_PARAMS.LON]
        radius = args[URL_PARAMS.RADIUS]

        _area_models = common.jsonutils.getAreaModels()

        area_model = common.jsonutils.getAreaModelByLocation(_area_models, lat, lon)
        model_data = common.utils.request_model_data_local(lat, lon, radius, start_datetime, end_datetime, area_model, outlier_filtering=False)
        return jsonify(model_data)