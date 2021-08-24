from common.params import URL_PARAMS, PARAMS_HELP_MESSAGES
from flask_restful import Resource
from flask_restful.reqparse import RequestParser 
from flask_restful.inputs import datetime_from_iso8601
from flask import jsonify
from datetime import datetime
import common.utils
import common.jsonutils
import numpy as np

# from common.decorators import processPreRequest

'''
slc_ut:     latHi=40.8206&latLo=40.481700000000004&lonHi=-111.7616&lonLo=-112.15939999999999
clev_oh:    latHi=41.5487&latLo=41.335800000000006&lonHi=-81.4179&lonLo=-81.9272
kc_mo:      latHi=39.196299999999994&latLo=38.885600000000004&lonHi=-94.47070000000001&lonLo=-94.7792
chatt_tn:   latHi=35.0899&latLo=34.9853&lonHi=-85.2101&lonLo=-85.37729999999999
pv_ma:      latHi=42.2405&latLo=42.0615&lonHi=-72.4681&lonLo=-72.6643
'''
arguments = RequestParser()
arguments.add_argument(URL_PARAMS.LAT_HI,   type=float,                 help=PARAMS_HELP_MESSAGES.LAT_HI,   required=True)
arguments.add_argument(URL_PARAMS.LAT_LO,   type=float,                 help=PARAMS_HELP_MESSAGES.LAT_LO,   required=True)
arguments.add_argument(URL_PARAMS.LON_HI,   type=float,                 help=PARAMS_HELP_MESSAGES.LON_HI,   required=True)
arguments.add_argument(URL_PARAMS.LON_LO,   type=float,                 help=PARAMS_HELP_MESSAGES.LON_LO,   required=True)
arguments.add_argument(URL_PARAMS.LAT_SIZE, type=int,                   help=PARAMS_HELP_MESSAGES.LAT_SIZE, required=True)
arguments.add_argument(URL_PARAMS.LON_SIZE, type=int,                   help=PARAMS_HELP_MESSAGES.LON_SIZE, required=True)
arguments.add_argument(URL_PARAMS.TIME,     type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.TIME,     required=True)
# arguments.add_argument(URL_PARAMS.START_TIME,    type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.START_TIME,    required=True)
# arguments.add_argument(URL_PARAMS.END_TIME,      type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.END_TIME,      required=True)
# arguments.add_argument(URL_PARAMS.TIME_INTERVAL, type=int,                   help=PARAMS_HELP_MESSAGES.TIME_INTERVAL, required=True, default=60)


class getEstimateMap(Resource):

    def get(self, **kwargs):
        
        args = arguments.parse_args()
        
        lat_hi = args[URL_PARAMS.LAT_HI]
        lat_lo = args[URL_PARAMS.LAT_LO]
        lon_hi = args[URL_PARAMS.LON_HI]
        lon_lo = args[URL_PARAMS.LON_LO]
        lat_size = args[URL_PARAMS.LAT_SIZE]
        lon_size = args[URL_PARAMS.LON_SIZE]
        query_date = args[URL_PARAMS.TIME]

        UTM = False

        _area_models = common.jsonutils.getAreaModels()

        if query_date == None:
            return "Not supported", 400
            # query_startdate = request.args.get('startTime')
            # query_enddate = request.args.get('endTime')
            # if (query_startdate == None) or (query_enddate == None):
            #     return 'requires valid date or start/end', 400
            # datesequence=True
            # try:
            #     query_rate = float(request.args.get('timeInterval', 0.25))
            # except ValueError:
            #     return 'timeInterval must be floats.', 400
        else:
            datesequence=False

        area_string = None
        # if "areaModel" in request.args:
        #     area_string = request.args.get('areaModel')
            
        area_model = common.jsonutils.getAreaModelByLocation(_area_models, lat=lat_hi, lon=lon_lo, string = area_string)
        if area_model == None:
            msg = f"The query location, lat={lat_hi}, lon={lon_lo}, and/or area string {area_string} does not have a corresponding area model"
            return msg, 400

    # build the grid of query locations
        if not UTM:
            # lon_vector, lat_vector = utils.interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_res, lon_res)
            lat_vector, lon_vector = common.utils.interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_size, lon_size)
    #        locations_UTM = utm.from_latlon(query_locations_latlon)
        else:
            # step 7.5, convert query box to UTM -- do the two far corners and hope for the best
    #        lat_lo_UTM, lon_lo_UTM, zone_num_lo, zone_let_lo = utils.latlonToUTM(lat_lo, lon_lo)
    #        lat_hi_UTM, lon_hi_UTM, zone_num_hi, zone_let_hi = utils.latlonToUTM(lat_hi, lon_hi)
    #        query_locations_UTM = utils.interpolateQueryLocations(lat_lo_UTM, lat_hi_UTM, lon_lo_UTM, lon_hi_UTM, spatial_res)
    #        query_locations_
            return 'UTM not yet supported', 400

    #    elevation_interpolator = jsonutils.buildAreaElevationInterpolator(area_model['elevationfile'])
    #   elevations = elevation_interpolator(lon_vector, lat_vector)
        locations_lon, locations_lat = np.meshgrid(lon_vector, lat_vector)
        query_lats = locations_lat.flatten()
        query_lons= locations_lon.flatten()
    #    query_elevations = elevations.flatten()
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
            return "Not Supported", 400
            # query_start_datetime = common.jsonutils.parseDateString(query_startdate, area_model['timezone'])
            # query_end_datetime = common.jsonutils.parseDateString(query_enddate, area_model['timezone'])
            # if query_start_datetime == None or query_end_datetime == None:
            #     msg = f"The query ({query_startdate}, {query_enddate}) is not a recognized date/time format; see also https://www.cl.cam.ac.uk/~mgk25/iso-time.html.  Default time zone is {area_model['timezone']}"
            #     return msg, 400
            # query_dates = utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_rate)


    #   # step 3, query relevent data
    #   # for this compute a circle center at the query volume.  Radius is related to lenth scale + the size fo the box.
    #     lat = (lat_lo + lat_hi)/2.0
    #     lon = (lon_lo + lon_hi)/2.0
    # #    NUM_METERS_IN_MILE = 1609.34
    # #    radius = latlon_length_scale / NUM_METERS_IN_MILE  # convert meters to miles for db query

    #     UTM_N_hi, UTM_E_hi, zone_num_hi, zone_let_hi = utils.latlonToUTM(lat_hi, lon_hi)
    #     UTM_N_lo, UTM_E_lo, zone_num_lo, zone_let_lo = utils.latlonToUTM(lat_lo, lon_lo)
    # # compute the lenght of the diagonal of the lat-lon box.  This units here are **meters**
    #     lat_diff = UTM_N_hi - UTM_N_lo
    #     lon_diff = UTM_E_hi - UTM_E_lo
    #     radius = SPACE_KERNEL_FACTOR_PADDING*latlon_length_scale + np.sqrt(lat_diff**2 + lon_diff**2)/2.0

    #     if not ((zone_num_lo == zone_num_hi) and (zone_let_lo == zone_let_hi)):
    #         return 'Requested region spans UTM zones', 400        

        yPred, yVar, query_elevations, status = common.utils.computeEstimatesForLocations(query_dates, query_locations, area_model)
        
        # yPred, yVar = gaussian_model_utils.estimateUsingModel(
        #     model, locations_lat, locations_lon, elevations, [query_datetime], time_offset)

        num_times = len(query_dates)

        print(f"elevations shape {query_elevations.shape}")
        query_elevations = query_elevations.reshape((lat_vector.shape[0], lon_vector.shape[0]))
        print(f"elevations shape {query_elevations.shape}")
        yPred = yPred.reshape((lat_vector.shape[0], lon_vector.shape[0], num_times))
        print(f"yPred shape {yPred.shape}")
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