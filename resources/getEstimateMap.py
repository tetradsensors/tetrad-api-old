from common.params import URL_PARAMS, PARAMS_HELP_MESSAGES
from flask_restful import Resource
from flask_restful.reqparse import RequestParser 
from flask_restful.inputs import datetime_from_iso8601
from flask import jsonify
from datetime import datetime
import common.utils
import common.jsonutils
import numpy as np

from common.decorators import processPreRequest

'''
slc_ut:     latHi=40.8206&latLo=40.481700000000004&lonHi=-111.7616&lonLo=-112.15939999999999
clev_oh:    latHi=41.5487&latLo=41.335800000000006&lonHi=-81.4179&lonLo=-81.9272
kc_mo:      latHi=39.196299999999994&latLo=38.885600000000004&lonHi=-94.47070000000001&lonLo=-94.7792
chatt_tn:   latHi=35.0899&latLo=34.9853&lonHi=-85.2101&lonLo=-85.37729999999999
pv_ma:      latHi=42.2405&latLo=42.0615&lonHi=-72.4681&lonLo=-72.6643
'''
arguments = RequestParser()
arguments.add_argument(URL_PARAMS.LAT_HI,        type=float,                 help=PARAMS_HELP_MESSAGES.LAT_HI,           required=True)
arguments.add_argument(URL_PARAMS.LAT_LO,        type=float,                 help=PARAMS_HELP_MESSAGES.LAT_LO,           required=True)
arguments.add_argument(URL_PARAMS.LON_HI,        type=float,                 help=PARAMS_HELP_MESSAGES.LON_HI,           required=True)
arguments.add_argument(URL_PARAMS.LON_LO,        type=float,                 help=PARAMS_HELP_MESSAGES.LON_LO,           required=True)
arguments.add_argument(URL_PARAMS.LAT_SIZE,      type=int,                   help=PARAMS_HELP_MESSAGES.LAT_SIZE,         required=True)
arguments.add_argument(URL_PARAMS.LON_SIZE,      type=int,                   help=PARAMS_HELP_MESSAGES.LON_SIZE,         required=True)
arguments.add_argument(URL_PARAMS.TIME,          type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.TIME,             required=False)
arguments.add_argument(URL_PARAMS.START_TIME,    type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.START_TIME,       required=False)
arguments.add_argument(URL_PARAMS.END_TIME,      type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.END_TIME,         required=False)
arguments.add_argument(URL_PARAMS.TIME_INTERVAL, type=float,                 help=PARAMS_HELP_MESSAGES.TIME_INTERVAL_HR, required=False)


class getEstimateMap(Resource):

    @processPreRequest
    def get(self, **kwargs):
        
        args = arguments.parse_args()
        
        lat_hi = args[URL_PARAMS.LAT_HI]
        lat_lo = args[URL_PARAMS.LAT_LO]
        lon_hi = args[URL_PARAMS.LON_HI]
        lon_lo = args[URL_PARAMS.LON_LO]
        lat_size = args[URL_PARAMS.LAT_SIZE]
        lon_size = args[URL_PARAMS.LON_SIZE]
        query_date = args[URL_PARAMS.TIME]
        query_start_datetime = args[URL_PARAMS.START_TIME]
        query_end_datetime = args[URL_PARAMS.END_TIME]
        query_rate = args[URL_PARAMS.TIME_INTERVAL]

        # Must include either time or (start, end, interval) but not both
        msg = f"Must include {URL_PARAMS.TIME} or all of: [{URL_PARAMS.START_TIME}, {URL_PARAMS.END_TIME}, {URL_PARAMS.TIME_INTERVAL}]. Cannot include {URL_PARAMS.TIME} and the others."
        if (query_date is None) and (not all((query_start_datetime, query_end_datetime, query_rate))):
            return {"message": msg}, 400
        if (query_date) and (any((query_start_datetime, query_end_datetime, query_rate))):
            return {"message": msg}, 400
        if all((query_date, query_start_datetime, query_end_datetime, query_rate)):
            return {"message": msg}, 400

        UTM = False

        _area_models = common.jsonutils.getAreaModels()

        datesequence = (query_date is None)
            
        area_model = common.jsonutils.getAreaModelByLocation(_area_models, lat=lat_hi, lon=lon_lo)
        if area_model == None:
            msg = f"The query location, lat={lat_hi}, lon={lon_lo} does not have a corresponding area model"
            return msg, 400

        # build the grid of query locations
        lat_vector, lon_vector = common.utils.interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_size, lon_size)

        locations_lon, locations_lat = np.meshgrid(lon_vector, lat_vector)
        query_lats = locations_lat.flatten()
        query_lons= locations_lon.flatten()
        query_locations = np.column_stack((query_lats, query_lons))

        # deal with single or time sequences.
        if not datesequence:
            if query_date == "now":
                query_date = (datetime.now()).strftime(common.jsonutils.DATETIME_FORMAT)
            query_datetime = query_date
            if query_datetime == None:
                msg = f"The query {query_date} is not a recognized date/time format or specify 'now'; see also https://www.cl.cam.ac.uk/~mgk25/iso-time.html.  Default time zone is {area_model['timezone']}"
                return msg, 400
            query_dates = np.array([query_datetime])
        else:
            query_dates = common.utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_rate)      

        yPred, yVar, query_elevations, status = common.utils.computeEstimatesForLocations(query_dates, query_locations, area_model)

        num_times = len(query_dates)

        query_elevations = query_elevations.reshape((lat_vector.shape[0], lon_vector.shape[0]))
        yPred = yPred.reshape((lat_vector.shape[0], lon_vector.shape[0], num_times))
        yVar = yVar.reshape((lat_vector.shape[0], lon_vector.shape[0], num_times))

        estimates = yPred.tolist()
        elevations = query_elevations.tolist()

        return_object = {
            "Area model": area_model["note"],
            "Elevations": elevations,
            "Latitudes":  lat_vector.tolist(),
            "Longitudes": lon_vector.tolist()
        }

        estimates = []
        for i in range(num_times):
            estimates.append(
                {
                    'PM2_5': (yPred[:,:,i]).tolist(), 
                    'Variance': (yVar[:,:,i]).tolist(), 
                    'Time': query_dates[i].strftime('%Y-%m-%d %H:%M:%S%z'), 
                    'Status': status[i]
                }
            )

        return_object['estimates'] = estimates
        
        return jsonify(return_object)