from common.params import URL_PARAMS, PARAMS_HELP_MESSAGES, lats_parse, lons_parse
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
arguments.add_argument(URL_PARAMS.LATS,          type=lats_parse,            help=PARAMS_HELP_MESSAGES.LATS,          required=True)
arguments.add_argument(URL_PARAMS.LONS,          type=lons_parse,            help=PARAMS_HELP_MESSAGES.LONS,          required=True)
arguments.add_argument(URL_PARAMS.TIME_INTERVAL, type=float,                 help=PARAMS_HELP_MESSAGES.TIME_INTERVAL, required=True)

class getEstimateAtLocations(Resource):

    @processPreRequest
    def get(self, **kwargs):
        args = arguments.parse_args()

        query_start_datetime = args[URL_PARAMS.START_TIME]
        query_end_datetime = args[URL_PARAMS.END_TIME]
        query_lats = args[URL_PARAMS.LATS]
        query_lons = args[URL_PARAMS.LONS]
        query_rate = args[URL_PARAMS.TIME_INTERVAL]

        _area_models = common.jsonutils.getAreaModels()

        area_model = common.jsonutils.getAreaModelByLocation(_area_models, query_lats[0], query_lons[0])
        if area_model == None:
            msg = f"The query location, lat={query_lats[0]}, lon={query_lons[0]}, does not have a corresponding area model"
            return msg, 400

        query_dates = common.utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_rate)

        if (query_lats.shape != query_lons.shape):
            return 'lat, lon must be equal sized arrays of floats:'+str(query_lats)+' ; ' + str(query_lons), 400

        query_dates = common.utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_rate)
        query_locations = np.column_stack((query_lats, query_lons))
        
        yPred, yVar, query_elevations, status = common.utils.computeEstimatesForLocations(query_dates, query_locations, area_model)

        num_times = len(query_dates)
        data_out = {'Latitude': query_lats.tolist(), 'Longitude': query_lons.tolist(), 'Elevation': query_elevations.tolist()}
        estimates = []

        for i in range(num_times):
            estimates.append(
                {
                    'PM2_5': (yPred[:,i]).tolist(), 
                    'Variance': (yVar[:,i]).tolist(), 
                    'Time': query_dates[i].strftime('%Y-%m-%d %H:%M:%S%z'), 
                    'Status': status[i]
                }
            )
        data_out["Estimates"] = estimates
        return jsonify(data_out)