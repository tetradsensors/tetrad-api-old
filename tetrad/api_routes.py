from datetime import datetime, timedelta
# import pytz
# from flask import request, jsonify
# import functools
# from google.cloud.bigquery import Client as BQClient, QueryJobConfig, ScalarQueryParameter
# from tetrad import app, cache, admin_utils, limiter, utils
# from tetrad.api_consts import *
# from tetrad.classes import ArgumentError, NoDataError
# # from tetrad import gaussian_model_utils
# import json
# import numpy as np 
# from os import getenv
# # import pandas as pd 
# import re 
# import requests
# from time import time 

from datetime import datetime, timedelta

from numpy.lib.arraysetops import isin
from tetrad import app, bq_client, bigquery, utils, elevation_interpolator, gaussian_model_utils, DEBUG
from tetrad.classes import *
if not DEBUG:
    import cache
import logging
# from dotenv import load_dotenv
from flask import request, jsonify
# regular expression stuff for decoding quer 
import re
import numpy as np

# import google.cloud.logging 
# gcloud_logging_client = google.cloud.logging.Client()
# gcloud_logging_client.get_default_handler()
# gcloud_logging_client.setup_logging()
# import logging

# AIRU_TABLE_ID = getenv("AIRU_TABLE_ID")
# PURPLEAIR_TABLE_ID = getenv("PURPLEAIR_TABLE_ID")
# DAQ_TABLE_ID = getenv("DAQ_TABLE_ID")
# SOURCE_TABLE_MAP = {
#     "AirU": AIRU_TABLE_ID,
#     "PurpleAir": PURPLEAIR_TABLE_ID,
#     "DAQ": DAQ_TABLE_ID,
# }
# VALID_SENSOR_SOURCES = ["AirU", "PurpleAir", "DAQ", "all"]

TIME_KERNEL_FACTOR_PADDING = 3.0
SPACE_KERNEL_FACTOR_PADDING = 2.
MIN_ACCEPTABLE_ESTIMATE = -5.0

# the size of time sequence chunks that are used to break the eatimation/data into pieces to speed up computation
# in units of time-scale parameter
# This is a tradeoff between looping through the data multiple times and having to do the fft inversion (n^2) of large time matrices
# If the bin size is 10 mins, and the and the time scale is 20 mins, then a value of 30 would give 30*20/10, which is a matrix size of 60.  Which is not that big.  
TIME_SEQUENCE_SIZE = 20.

@app.errorhandler(ArgumentError)
def handle_arg_error(error):
    d = error.to_dict()
    d['message'] += ' For more information, please visit: https://github.com/tetradsensors/tetrad_site'
    response = jsonify(d)
    response.status_code = error.status_code
    return response


@app.errorhandler(NoDataError)
def handle_nodata_error(error):
    d = error.to_dict()
    d['message'] += ' For more information, please visit: https://github.com/tetradsensors/tetrad_site'
    response = jsonify(d)
    response.status_code = error.status_code
    return response

# bq_client = BQClient()

# @app.route("/", subdomain=getenv('SUBDOMAIN_API'))
# def home():
#     return "Tetrad API Landing Page Placeholder"

# https://api.tetradsensors.com/liveSensors?src=all&field=pm2_5

# @app.route("/liveSensors", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
@app.route("/liveSensors", methods=["GET"])
# # @cache.cached(timeout=119)
def liveSensors():

    def argParseDelta(delta):
        delta = delta or (24 * 60)
        if delta <= 0:
            raise ArgumentError("Argument 'delta' must be a positive integer (minutes)", 400)
        return delta

    req_args = [
        'src',
        'field'
    ]

    #################################
    # Arg parse
    #################################

    try:
        utils.verifyRequiredArgs(request.args, req_args)
        srcs = 'all'
        # srcs   = utils.argParseSources(request.args.get('src', type=str))
        fields = utils.argParseFields(request.args.get('field', type=str))
        delta  = argParseDelta(request.args.get('delta', type=int))
    except ArgumentError:
        raise

    #################################
    # Query Builder
    #################################

    Q_FIELDS = utils.queryBuildFields(fields)
    Q_LABELS = utils.queryBuildLabels(srcs)
    # Build the query
    SUBQ = f"""SELECT 
                    {Q_FIELDS}
                FROM 
                    `telemetry.telemetry`
                WHERE 
                    {Q_LABELS}
                        AND
                    Timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {delta} MINUTE)
            """

    # Build the full query
    q = f"""
    SELECT 
        * EXCEPT(row_num)
    FROM
        (
            SELECT 
                *, 
                ROW_NUMBER() 
            OVER
                (
                    PARTITION BY 
                        DeviceID 
                    ORDER BY 
                        Timestamp DESC
                ) row_num
            FROM 
                (
                    {SUBQ}
                )
        )
    WHERE 
        row_num = 1;
    """

    query_job = bq_client.query(q)
    rows = query_job.result()

    data = [dict(r) for r in rows]

    # Clean data and apply correction factors
    # data = utils.tuneAllFields(data, fields)
    data = [d for d in data if d['PM2_5'] is not None and d['PM2_5'] < 500]

    return jsonify(data), 200

# # https://api.tetradsensors.com/requestData?src=slc_ut&field=pm2_5&start=2021-01-01T00:00:00Z&end=2021-01-22T00:00:00Z
# @app.route("/requestData", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
# # @app.route("/requestData", methods=["GET"])
# def requestData():
#     """
#     Arguments:
#     @param: qtype   (required)  
#     @param: field   (required)
#     @param: start   (required)
#     @param: end     (required)
#     @param: devices (optional)  Single device or list of devices
#     @param: box     (optional)  List of coordinates in this order: North, South, East, West
#     @param: radius  (optional)  Radius in kilometers
#     @param: center  (optional)  Required if 'radius' is supplied. Lat,Lon center of radius. &center=42.012,-111.423&
#     """

#     args = [
#         'src',
#         'field',
#         'start',
#         'end',
#         'device',
#         'box',
#         'radius',
#         'center'
#     ]

#     req_args = [
#         'field', 
#         'start', 
#         'end',
#     ]
#     # You don't have to include 'src' if 'device' is here
#     if not request.args.get('device', type=str):
#         req_args.append('src')

#     try:
#         utils.verifyArgs(request.args, req_args, args)
        
#         # Required
#         srcs    = utils.argParseSources(request.args.get('src', type=str), canBeNone=True)
#         fields  = utils.argParseFields(request.args.get('field', type=str))
#         start   = utils.argParseDatetime(request.args.get('start', type=str))
#         end     = utils.argParseDatetime(request.args.get('end', type=str))
        
#         # Optional
#         devices = utils.argParseDevices(request.args.get('device', type=str))
#         box     = utils.argParseBBox(request.args.get('box', type=str))
#         rc      = utils.argParseRadiusArgs(
#                     request.args.get('radius', type=float),
#                     request.args.get('center', type=str))
#     except ArgumentError as e:
#         raise
#     except Exception as e:
#         logging.error(str(e))
#         raise

#     #################################
#     # Query Picker
#     #################################
#     data = None
#     if box and rc:
#         raise ArgumentError("Must choose either 'box' or 'radius','center' arguments", status_code=400)
#     elif rc:
#         data = _requestData(srcs, fields, start, end, radius=rc[0], center=rc[1], id_ls=devices)
#     else:
#         data = _requestData(srcs, fields, start, end, bbox=box, id_ls=devices)

#     if isinstance(data, int):
#         if data == 408:
#             return "Timeout (2 minutes). Try a smaller query.", 408
#         else:
#             return "Something went wrong. That's all we know. Contact the developers.", data

#     response = jsonify(data)
#     response.status_code = 200
#     return response


# def _requestData(srcs, fields, start, end, bbox=None, radius=None, center=None, id_ls=None, removeNulls=False):
#     """
#     Function to query a field (like Temperature, Humidity, PM, etc.) 
#     or list of fields, in date range [start, end], inside a bounding
#     box. The bounding box is a dict {'lat_hi', 'lat_lo', 'lon_hi', 'lon_lo'} 
#     coordinates.
#     If radius, radius is in meters, center is dict {'lat', 'lon'}
#     Can include an ID or a list of IDs
#     """

#     if id_ls:
#         query_devs = utils.idsToWHEREClause(id_ls, FIELD_MAP['DEVICEID'])
#     else:
#         query_devs = "True"

#     query_fields = utils.queryBuildFields(fields)

#     if bbox:
#         n, s, e, w = bbox['lat_hi'], bbox['lat_lo'], bbox['lon_hi'], bbox['lon_lo']
#         query_region = f"""
#             ST_WITHIN(
#                 {FIELD_MAP["GPS"]}, 
#                 ST_GeogFromGeoJSON(
#                     '{{"type": "Polygon", "coordinates": [[[{e},{n}],[{e},{s}],[{w},{s}],[{w},{n}],[{e},{n}]]]}}'
#                 )
#             )
#         """
#     elif radius:
#         query_region = f"""
#             ST_DWITHIN(
#                 {FIELD_MAP["GPS"]}, 
#                 ST_GEOGPOINT({center['lon']}, {center['lat']}),
#                 {radius * 1000}
#             )
#         """
#     else:
#         query_region = "True"

#     query_labels = utils.queryBuildLabels(srcs)
#     QUERY = f"""
#         SELECT 
#             {query_fields}
#         FROM 
#             `{BQ_PATH_TELEMETRY}` 
#         WHERE 
#             {query_labels}
#                 AND
#             {FIELD_MAP["TIMESTAMP"]} >= "{start}"
#                 AND
#             {FIELD_MAP["TIMESTAMP"]} <= "{end}"
#                 AND
#             {query_region}
#                 AND
#             {query_devs}
#         ORDER BY
#             {FIELD_MAP["TIMESTAMP"]}
#     """

#     # data = ' '.join([i for i in QUERY.replace('\n', ' ').split(' ') if i])

#     # Run the query and collect the result
#     # try:
#     query_job = bq_client.query(QUERY)
#     rows = query_job.result()   
#     # except Exception as e:
#     #     print(str(e))
#     #     return 408
    
#     # # break on empty iterator
#     if rows.total_rows == 0:
#         raise NoDataError("No data returned.", status_code=222)
        
#     # # Convert Response object (generator) to list-of-dicts
#     data = [dict(r) for r in rows]

#     # # Clean data and apply correction factors
#     # data = utils.tuneAllFields(data, fields, removeNulls=removeNulls)

#     # Apply correction factors to data
    
#     return data


# @app.route("/nickname", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
# @app.route("/nickname", methods=["GET"])
# def nickname():

#     args = [
#         'device',
#         'nickname'
#     ]

#     req_args = args

#     try:
#         utils.verifyArgs(request.args, req_args, args)
#         device = utils.argParseDevices(request.args.get('device', type=str), single_device=True)

#         # nicknames
#         nickname = request.args.get('nickname')
#         if not re.match(r'^[ -~]{1,128}$', nickname):
#             raise ArgumentError(f"Parameter 'nickname' must be be between 1 and 128 ASCII characters.")
        
#     except ArgumentError as e:
#         raise

#     # Perform the UPDATE query
#     query = f'''
#     UPDATE
#         `{PROJECT_ID}.{getenv('BQ_DATASET_META')}.{getenv('BQ_TABLE_META_DEVICES')}`
#     SET
#         {getenv('FIELD_NN')} = "{nickname}"
#     WHERE
#         {getenv('FIELD_ID')} = "{device}"
#     '''

#     print(query)
#     bq_client.query(query)

#     return 'success', 200


# http://127.0.0.1:8080/getEstimateMap?latLo=39.4&latHi=41.1&lonLo=-112&lonHi=-111.8&date=2021-05-26T15:00:00Z&latSize=20&lonSize=20
@app.route("/getEstimateMap", methods=["GET"])
def getEstimateMap():

    # this species grid positions should be interpolated in UTM coordinates
    # right now (Nov 2020) this is not supported.
    # might be used later in order to build grids of data in UTM coordinates -- this would depend on what the display/visualization code needs
    # after investigation, most vis toolkits support lat-lon grids of data. 
    if "UTM" in request.args:
        UTM = True
    else:
        UTM = False

    # Get the arguments from the query string
    if not UTM:
        try:
            lat_hi = float(request.args.get('latHi'))
            lat_lo = float(request.args.get('latLo'))
            lon_hi = float(request.args.get('lonHi'))
            lon_lo = float(request.args.get('lonLo'))
        except ValueError:
            return 'lat, lon, lat_res, be floats in the lat-lon (not UTM) case', 400
        try:
            lat_size = int(request.args.get('latSize'))
            lon_size = int(request.args.get('lonSize'))
        except ValueError:
            return 'lat, lon, sizes must be ints (not UTM) case', 400

        lat_res = (lat_hi-lat_lo)/float(lat_size)
        lon_res = (lon_hi-lon_lo)/float(lon_size)

    query_date = request.args.get('date')
    if not utils.validateDate(query_date):
        msg = f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
        return msg, 400

    query_datetime = utils.parseDateString(query_date)

    app.logger.info((
        f"Query parameters: lat_lo={lat_lo} lat_hi={lat_hi}  lon_lo={lon_lo} lon_hi={lon_hi} lat_res={lat_res} lon_res={lon_res} date={query_datetime}"
    ))

    # # step 0, load up the bounding box from file and check that request is within it
    # bounding_box_vertices = utils.loadBoundingBox('bounding_box.csv')
    # print(f'Loaded {len(bounding_box_vertices)} bounding box vertices.')

    # if not (
    #     utils.isQueryInBoundingBox(bounding_box_vertices, lat_lo, lon_lo) and
    #     utils.isQueryInBoundingBox(bounding_box_vertices, lat_lo, lon_hi) and
    #     utils.isQueryInBoundingBox(bounding_box_vertices, lat_hi, lon_hi) and
    #     utils.isQueryInBoundingBox(bounding_box_vertices, lat_hi, lon_lo)):
    #     return 'One of the query locations is outside of the bounding box for the database', 400

        
# build the grid of query locations
    if not UTM:
        lon_vector, lat_vector = utils.interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_res, lon_res)
#        locations_UTM = utm.from_latlon(query_locations_latlon)
    else:
        # step 7.5, convert query box to UTM -- do the two far corners and hope for the best
#        lat_lo_UTM, lon_lo_UTM, zone_num_lo, zone_let_lo = utils.latlonToUTM(lat_lo, lon_lo)
#        lat_hi_UTM, lon_hi_UTM, zone_num_hi, zone_let_hi = utils.latlonToUTM(lat_hi, lon_hi)
#        query_locations_UTM = utils.interpolateQueryLocations(lat_lo_UTM, lat_hi_UTM, lon_lo_UTM, lon_hi_UTM, spatial_res)
#        query_locations_
        return 'UTM not yet supported', 400

    elevations = elevation_interpolator(lon_vector, lat_vector)
    locations_lon, locations_lat = np.meshgrid(lon_vector, lat_vector)
    query_lats = locations_lat.flatten()
    query_lons= locations_lon.flatten()
    query_elevations = elevations.flatten()
    query_dates = np.array([query_datetime])
    query_locations = np.column_stack((query_lats, query_lons))    

#   # step 3, query relevent data
#   # for this compute a circle center at the query volume.  Radius is related to lenth scale + the size fo the box.
#     lat = (lat_lo + lat_hi)/2.0
#     lon = (lon_lo + lon_hi)/2.0
# #    NUM_METERS_IN_MILE = 1609.34
# #    radius = latlon_length_scale / NUM_METERS_IN_MILE  # convert meters to miles for db query

#     UTM_N_hi, UTM_E_hi, zone_num_hi, zone_let_hi = utils.latlonToUTM(lat_hi, lon_hi)
#     UTM_N_lo, UTM_E_l, zone_num_lo, zone_let_lo = utils.latlonToUTM(lat_lo, lon_lo)
# # compute the lenght of the diagonal of the lat-lon box.  This units here are **meters**
#     lat_diff = UTM_N_hi - UTM_N_lo
#     lon_diff = UTM_E_hi - UTM_E_lo
#     radius = SPACE_KERNEL_FACTOR_PADDING*latlon_length_scale + np.sqrt(lat_diff**2 + lon_diff**2)/2.0

#     if not ((zone_num_lo == zone_num_hi) and (zone_let_lo == zone_let_hi)):
#         return 'Requested region spans UTM zones', 400        

    ret = computeEstimatesForLocations(query_dates, query_locations, query_elevations)

    if isinstance(ret[0], str):
        print("there was an error:", ret)
        return ret
    else:
        yPred, yVar, status = ret
    
    # yPred, yVar = gaussian_model_utils.estimateUsingModel(
    #     model, locations_lat, locations_lon, elevations, [query_datetime], time_offset)

    elevations = (elevations).tolist()
    yPred = yPred.reshape((lat_size, lon_size))
    yVar = yVar.reshape((lat_size, lon_size))
    estimates = yPred.tolist()
    variances = yVar.tolist()
    return jsonify({"Elevations":elevations, "PM2.5":estimates, "PM2.5 variance":variances, "Latitudes":lat_vector.tolist(), "Longitudes":lon_vector.tolist()})

# this is a generic helper function that sets everything up and runs the model
def computeEstimatesForLocations(query_dates, query_locations, query_elevations):
    num_locations = query_locations.shape[0]
    query_lats = query_locations[:,0]
    query_lons = query_locations[:,1]
    query_start_datetime = query_dates[0]
    query_end_datetime = query_dates[-1]

            
    # step 0, load up the bounding box from file and check that request is within it
    # bounding_box_vertices = utils.loadBoundingBox('bounding_box.csv')
    
    region = utils.getRegionContainingPoint(lat=query_lats[0], lon=query_lons[0])
    if region is None:
        print("No region found")
        return "Not inside a valid region", 400
    else:
        region_name, region_info = region
        print('Found the region:', region_name)

    bounding_box_vertices = utils.getBoundingBoxFromRegion(region_info)
    app.logger.info("Loaded " + str(len(bounding_box_vertices)) + " bounding box vertices.")

    for i in range(num_locations):
        if not utils.isQueryInBoundingBox(bounding_box_vertices, query_lats[i], query_lons[i]):
            return f'The query location, {query_lats[i]},{query_lons[i]},  is outside of the bounding box\n\n{bounding_box_vertices}\n\n', 400

    # step 1, load up correction factors from file
    # correction_factors = utils.loadCorrectionFactors('correction_factors.csv')
    correction_factors = utils.getCorrectionFactorsFromRegion(region_info)
    app.logger.debug(f'Loaded {len(correction_factors)} correction factors.')

    # step 2, load up length scales from file
    length_scales = utils.loadLengthScales('length_scales.csv')
    app.logger.debug("Loaded length scales: " + str(length_scales))

    length_scales = utils.getScalesInTimeRange(length_scales, query_start_datetime, query_end_datetime)
    if len(length_scales) < 1:
        msg = (
            f"Incorrect number of length scales({len(length_scales)}) "
            f"found in between {query_start_datetime} and {query_end_datetime}"
        )
        return msg, 400

    latlon_length_scale = length_scales[0]['latlon']
    elevation_length_scale = length_scales[0]['elevation']
    time_length_scale = length_scales[0]['time']

    app.logger.debug(f'Using length scales: latlon={latlon_length_scale} elevation={elevation_length_scale} time={time_length_scale}')

    # step 3, query relevent data

# these conversions were when we were messing around with specifying radius in miles and so forth.      
#    NUM_METERS_IN_MILE = 1609.34
#    radius = latlon_length_scale / NUM_METERS_IN_MILE  # convert meters to miles for db query

#    radius = latlon_length_scale / 70000


# radius is in meters, as is the length scale and UTM.    
    radius = SPACE_KERNEL_FACTOR_PADDING*latlon_length_scale

    sensor_data = request_model_data_local(
        query_lats,
        query_lons,
        radius=radius,
        start_date=query_start_datetime - timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale),
        end_date=query_end_datetime + timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale))


    unique_sensors = {datum['ID'] for datum in sensor_data}
    app.logger.info(f'Loaded {len(sensor_data)} data points for {len(unique_sensors)} unique devices from bgquery.')

    # step 3.5, convert lat/lon to UTM coordinates
    try:
        utils.convertLatLonToUTM(sensor_data)
    except ValueError as err:
        return f'{str(err)}', 400

    # TOM says: I don't know what this is for, I'm removing it
    # sensor_data = [datum for datum in sensor_data if datum['zone_num'] == 12]

    unique_sensors = {datum['ID'] for datum in sensor_data}
    app.logger.info((
        "After removing points with zone num != 12: "
        f"{len(sensor_data)} data points for {len(unique_sensors)} unique devices."
    ))

    # Step 4, parse sensor type from the version
    sensor_source_to_type = {'Tetrad': '3003', 'AQ&U': '3003', 'PurpleAir': '5003', 'DAQ': '0000'}
# DAQ does not need a correction factor
    print('add pm sensor type')
    for datum in sensor_data:
        datum['type'] =  sensor_source_to_type[datum['SensorSource']]

    app.logger.info(f'Fields: {sensor_data[0].keys()}')

    # step 4.5, Data Screening
#    print('Screening data')
    sensor_data = utils.removeInvalidSensors(sensor_data)

    # step 5, apply correction factors to the data
    print('correction factors...')
    for datum in sensor_data:
        # datum['PM2_5'] = utils.applyCorrectionFactor(correction_factors, datum['time'], datum['PM2_5'], datum['type'])
        datum['PM2_5'] = utils.applyCorrectionFactor2(correction_factors, datum['time'], datum['PM2_5'], datum['type'])

    # step 6, add elevation values to the data
    # NOTICE - the elevation object takes locations in the form "lon-lat"
    print('altitude')
    for datum in sensor_data:
        if 'Altitude' not in datum:
            datum['Altitude'] = elevation_interpolator([datum['Longitude']],[datum['Latitude']])[0]

    # This does the calculation in one step --- old method --- less efficient.  Below we break it into pieces.  Remove this once the code below (step 7) is fully tested.
    # step 7, get estimates from model
    # # step 8, Create Model
    # model, time_offset = gaussian_model_utils.createModel(
    #     sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale)
    # # step 9, Compute estimates
    # yPred, yVar = gaussian_model_utils.estimateUsingModel(
    #     model, query_lats, query_lons, query_elevations, query_dates, time_offset)

    
    time_padding = timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale)
    time_sequence_length = timedelta(hours = TIME_SEQUENCE_SIZE*time_length_scale)
    sensor_sequence, query_sequence = utils.chunkTimeQueryData(query_dates, time_sequence_length, time_padding)

    yPred = np.empty((num_locations, 0))
    yVar = np.empty((num_locations, 0))
    status = []
    for i in range(len(query_sequence)):
    # step 7, Create Model
        model, time_offset, model_status = gaussian_model_utils.createModel(
            sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale, sensor_sequence[i][0], sensor_sequence[i][1], save_matrices=False)
        # check to see if there is a valid model
        if (model == None):
            yPred_tmp = np.full((query_lats.shape[0], len(query_sequence[i])), 0.0)
            yVar_tmp = np.full((query_lats.shape[0], len(query_sequence[i])), np.nan)
            status_estimate_tmp = [model_status for i in range(len(query_sequence[i]))]
        else:
            yPred_tmp, yVar_tmp, status_estimate_tmp = gaussian_model_utils.estimateUsingModel(
                model, query_lats, query_lons, query_elevations, query_sequence[i], time_offset, save_matrices=True)
        # put the estimates together into one matrix
        yPred = np.concatenate((yPred, yPred_tmp), axis=1)
        yVar = np.concatenate((yVar, yVar_tmp), axis=1)
        status = status + status_estimate_tmp

    if np.min(yPred) < MIN_ACCEPTABLE_ESTIMATE:
        app.logger.warn("got estimate below level " + str(MIN_ACCEPTABLE_ESTIMATE))
        
# Here we clamp values to ensure that small negative values to do not appear
    yPred = np.clip(yPred, a_min = 0., a_max = None)

    return yPred, yVar, status

# could do an ellipse in lat/lon around the point using something like this
#WHERE SQRT(POW(Latitude - @lat, 2) + POW(Longitude - @lon, 2)) <= @radius
#    AND time > @start_date AND time < @end_date
#    ORDER BY time ASC
# Also could do this by spherical coordinates on the earth -- however we use a lat-lon box to save compute time on the BigQuery server

# radius should be in *meters*!!!
# this has been modified so that it now takes an array of lats/lons
# the radius parameter is not implemented in a precise manner -- rather it is converted to a lat-lon bounding box and all within that box are returned
# there could be an additional culling of sensors outside the radius done here after the query - if the radius parameter needs to be precise. 
def request_model_data_local(lats, lons, radius, start_date, end_date):
    model_data = []
    # get the latest sensor data from each sensor
    # Modified by Ross for
    ## using a bounding box in lat-lon
    if isinstance(lats, (float)):
            if isinstance(lons, (float)):
                    lat_lo, lat_hi, lon_lo, lon_hi = utils.latlonBoundingBox(lats, lons, radius)
            else:
                    return "lats,lons data structure misalignment in request sensor data", 400
    elif (isinstance(lats, (np.ndarray)) and isinstance(lons, (np.ndarray))):
        if not lats.shape == lons.shape:
            return "lats,lons data data size error", 400
        else:
            num_points = lats.shape[0]
            lat_lo, lat_hi, lon_lo, lon_hi = utils.latlonBoundingBox(lats[0], lons[0], radius)
            for i in range(1, num_points):
                lat_lo, lat_hi, lon_lo, lon_hi = utils.boundingBoxUnion((utils.latlonBoundingBox(lats[i], lons[i], radius)), (lat_lo, lat_hi, lon_lo, lon_hi))
    else:
        return "lats,lons data structure misalignment in request sensor data", 400
    app.logger.info("Query bounding box is %f %f %f %f" %(lat_lo, lat_hi, lon_lo, lon_hi))

   
    print('querying...')
    rows = submit_sensor_query(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date)
    print('done...')
    for row in rows:
        model_data.append({
            "ID": str(row.DeviceID),
            "Latitude": row.Latitude,
            "Longitude": row.Longitude,
            "time": row.Timestamp,
            "PM2_5": row.PM2_5,
            # "SensorModel": row.SensorModel,
            "SensorSource": row.Source,
        })
    print('done converting')

    return model_data


# submit a query for a range of values
# Ross Nov 2020
# this has been consolidate and generalized so that multiple api calls can use the same query code
def submit_sensor_query(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date):
    # query = f"""
    # SELECT *
    # FROM
    # (
    #     (
    #         SELECT ID, time, PM2_5, Latitude, Longitude, SensorModel, 'AirU' as SensorSource
    #         FROM `{AIRU_TABLE_ID}`
    #         WHERE time > @start_date AND time < @end_date
    #     )
    #     UNION ALL
    #     (
    #         SELECT ID, time, PM2_5, Latitude, Longitude, "" as SensorModel, 'PurpleAir' as SensorSource
    #         FROM `{PURPLEAIR_TABLE_ID}`
    #         WHERE time > @start_date AND time < @end_date
    #     )
    #     UNION ALL
    #     (
    #         SELECT ID, time, PM2_5, Latitude, Longitude, '' as SensorModel, 'DAQ' as SensorSource
    #         FROM `{DAQ_TABLE_ID}`
    #         WHERE time > @start_date AND time < @end_date
    #     )
    # ) WHERE (Latitude <= @lat_hi) AND (Latitude >= @lat_lo) AND (Longitude <= @lon_hi) AND (Longitude >= @lon_lo) AND time > @start_date AND time < @end_date
    # ORDER BY time ASC
    # """

    query = f"""
        SELECT
            DeviceID,
            Timestamp,
            PM2_5,
            ST_Y(GPS) AS Latitude,
            ST_X(GPS) AS Longitude,
            Source
        FROM
            `telemetry.telemetry`
        WHERE
            ST_WITHIN(
                GPS, 
                ST_GeogFromGeoJSON(
                    '{{"type": "Polygon", "coordinates": [[[{lon_hi},{lat_hi}],[{lon_hi},{lat_lo}],[{lon_lo},{lat_lo}],[{lon_lo},{lat_hi}],[{lon_hi},{lat_hi}]]]}}'
                )
            )
                AND
            Timestamp > @start_date
                AND
            Timestamp < @end_date
    """

    
# Old code that does not compute distance correctly
#    WHERE SQRT(POW(Latitude - @lat, 2) + POW(Longitude - @lon, 2)) <= @radius
#    AND time > @start_date AND time < @end_date
#    ORDER BY time ASC
#    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            # bigquery.ScalarQueryParameter("lat", "NUMERIC", lat),
            # bigquery.ScalarQueryParameter("lon", "NUMERIC", lon),
            # bigquery.ScalarQueryParameter("radius", "NUMERIC", radius),
            bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
            bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            # bigquery.ScalarQueryParameter("lat_lo", "NUMERIC", lat_lo),
            # bigquery.ScalarQueryParameter("lat_hi", "NUMERIC", lat_hi),
            # bigquery.ScalarQueryParameter("lon_lo", "NUMERIC", lon_lo),
            # bigquery.ScalarQueryParameter("lon_hi", "NUMERIC", lon_hi),
        ]
    )

    query_job = bq_client.query(query, job_config=job_config)

    if query_job.error_result:
        app.logger.error(query_job.error_result)
        return "Invalid API call - check documentation.", 400
    # Waits for query to finish
    sensor_data = query_job.result()  

    return sensor_data

