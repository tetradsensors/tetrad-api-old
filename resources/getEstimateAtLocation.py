from common.params import URL_PARAMS, PARAMS_HELP_MESSAGES, lat_check, lon_check
from flask_restful import Resource
from flask_restful.reqparse import RequestParser 
from flask_restful.inputs import datetime_from_iso8601
from flask import jsonify
import common.utils
import common.jsonutils
import numpy as np

from common.decorators import processPreRequest

arguments = RequestParser()
arguments.add_argument(URL_PARAMS.START_TIME,    type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.START_TIME,    required=True)
arguments.add_argument(URL_PARAMS.END_TIME,      type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.END_TIME,      required=True)
arguments.add_argument(URL_PARAMS.LAT,           type=lat_check,             help=PARAMS_HELP_MESSAGES.LAT,           required=True)
arguments.add_argument(URL_PARAMS.LON,           type=lon_check,             help=PARAMS_HELP_MESSAGES.LON,           required=True)
arguments.add_argument(URL_PARAMS.TIME_INTERVAL, type=float,                 help=PARAMS_HELP_MESSAGES.TIME_INTERVAL, required=True)

class getEstimateAtLocation(Resource):

    @processPreRequest
    def get(self, **kwargs):
        args = arguments.parse_args()

        query_start_datetime = args[URL_PARAMS.START_TIME]
        query_end_datetime = args[URL_PARAMS.END_TIME]
        query_lat = args[URL_PARAMS.LAT]
        query_lon = args[URL_PARAMS.LON]
        query_rate = args[URL_PARAMS.TIME_INTERVAL]

        _area_models = common.jsonutils.getAreaModels()

        area_model = common.jsonutils.getAreaModelByLocation(_area_models, query_lat, query_lon)
        if area_model == None:
            msg = f"The query location, lat={query_lat}, lon={query_lon}, does not have a corresponding area model"
            return msg, 400

        query_dates = common.utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_rate)
        
        query_locations = np.column_stack((np.array((query_lat)), np.array((query_lon))))

        yPred, yVar, query_elevations, status = common.utils.computeEstimatesForLocations(query_dates, query_locations, area_model)
        
        num_times = len(query_dates)
        estimates = []
        for i in range(num_times):
            estimates.append(
                {
                    'PM2_5': (yPred[0, i]), 
                    'Variance': (yVar[0, i]), 
                    'Time': query_dates[i].strftime('%Y-%m-%d %H:%M:%S%z'), 
                    'Latitude': query_lat, 
                    'Longitude': query_lon, 
                    'Elevation': query_elevations[0], 
                    'Status': status[i]
                }
            )

        return jsonify(estimates)