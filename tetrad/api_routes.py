from datetime import datetime, timedelta
from flask import request, jsonify
import functools
from google.cloud.bigquery import Client, QueryJobConfig, ScalarQueryParameter
from tetrad import app, cache, admin_utils, limiter, utils
from tetrad.api_consts import *
from tetrad.classes import ArgumentError, NoDataError
# from tetrad import gaussian_model_utils
import json
import numpy as np 
from os import getenv
import pandas as pd 
import re 
import requests
from time import time 
import google.cloud.logging 
gcloud_logging_client = google.cloud.logging.Client()
gcloud_logging_client.get_default_handler()
gcloud_logging_client.setup_logging()
import logging


@app.errorhandler(ArgumentError)
def handle_arg_error(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code 
    return response 

bq_client = Client(project=PROJECT_ID)
elevation_interpolator = utils.setupElevationInterpolator()

# https://api.tetradsensors.com/liveSensors?src=all&field=pm2_5
@app.route("/liveSensors", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
# @cache.cached(timeout=119)
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
        srcs   = utils.argParseSources(request.args.get('src', type=str))
        fields = utils.argParseFields(request.args.get('field', type=str))
        delta  = argParseDelta(request.args.get('delta', type=int))
    except ArgumentError:
        raise

    #################################
    # Query Builder
    #################################

    Q_FIELDS = utils.queryBuildFields(fields)

    # Build the 'source tables' portion of query
    Q_TBL = f"""SELECT 
                    {Q_FIELDS}
                FROM 
                    `{PROJECT_ID}.{BQ_DATASET_TELEMETRY}.%s`
                WHERE 
                    {FIELD_MAP["TIMESTAMP"]} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {delta} MINUTE)
            """

    # Union all the subqueries
    tbl_union = utils.queryBuildSources(srcs, Q_TBL)

    # Build the full query
    q = f"""
    SELECT 
        {Q_FIELDS}
    FROM
        (
            SELECT 
                *, 
                ROW_NUMBER() 
            OVER
                (
                    PARTITION BY 
                        {FIELD_MAP["DEVICEID"]} 
                    ORDER BY 
                        {FIELD_MAP["TIMESTAMP"]} DESC
                ) row_num
            FROM 
                (
                    {tbl_union}
                )
        )
    WHERE 
        row_num = 1;
    """

    query_job = bq_client.query(q)
    rows = query_job.result()

    data = [dict(r) for r in rows]

    # Clean data and apply correction factors
    data = utils.tuneData(data, fields)

    return jsonify(data), 200


@app.route("/requestData", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
def requestData():
    """
    Arguments:
    @param: qtype   (required)  
    @param: field   (required)
    @param: start   (required)
    @param: end     (required)
    @param: devices (optional)  Single device or list of devices
    @param: bbox    (optional)  List of coordinates in this order: North, South, East, West
    @param: radius  (optional)  Radius in Kilometers
    """

    req_args = [
        'src', 
        'field', 
        'start', 
        'end',
    ]

    try:
        utils.verifyRequiredArgs(request.args, req_args)
        
        # Required
        srcs    = utils.argParseSources(request.args.get('src', type=str))
        fields  = utils.argParseFields(request.args.get('field', type=str))
        start   = utils.argParseDatetime(request.args.get('start', type=str))
        end     = utils.argParseDatetime(request.args.get('end', type=str))
        
        # Optional
        devices = utils.argParseDevices(request.args.get('devices', type=str))
        box     = utils.argParseBBox(request.args.getlist('box', type=str))
        rc      = utils.argParseRadiusArgs(
                    request.args.get('radius', type=float),
                    request.args.get('center', type=str))
    except ArgumentError as e:
        raise
    except Exception as e:
        logging.error(str(e))

    #################################
    # Query Picker
    #################################
    data = None
    if box and rc:
        raise ArgumentError("Must choose either 'box' or 'radius' and 'center' arguments", status_code=400)
    elif rc:
        data = _requestDataInRadius(srcs, fields, start, end, radius=rc[0], center=rc[1], id_ls=devices)
    else:
        data = _requestData(srcs, fields, start, end, bbox=box, id_ls=devices)

    return jsonify(data), 200


def _requestData(srcs, fields, start, end, bbox=None, id_ls=None):
    """
    Function to query a field (like Temperature, Humidity, PM, etc.) 
    or list of fields, in date range [start, end], inside a bounding
    box. The bounding box is a tuple of (North,South,East,West) 
    coordinates.
    Can include an ID or a list of IDs
    """

    if id_ls:
        idstr = utils.idsToWHEREClause(id_ls, FIELD_MAP['DEVICEID'])
    else:
        idstr = "True"

    query_fields = utils.queryBuildFields(fields)

    if bbox:
        query_latlon = f"""
            {FIELD_MAP["LATITUDE"]}  <= "{bbox[0]}"
                AND
            {FIELD_MAP["LATITUDE"]}  >= "{bbox[1]}"
                AND
            {FIELD_MAP["LONGITUDE"]} <= "{bbox[2]}"
                AND
            {FIELD_MAP["LONGITUDE"]} >= "{bbox[3]}"
        """
    else:
        query_latlon = "True"

    Q_TBL = f"""
        SELECT 
            {query_fields}
        FROM 
            `{PROJECT_ID}.{BQ_DATASET_TELEMETRY}.%s` 
        WHERE 
            {FIELD_MAP["TIMESTAMP"]} >= "{start}"
                AND
            {FIELD_MAP["TIMESTAMP"]} <= "{end}"
                AND
            {query_latlon}   
    """

    tbl_union = utils.queryBuildSources(srcs, Q_TBL)

    # Build the query
    q = f"""
        SELECT
            {query_fields}
        FROM 
            ({tbl_union})
        WHERE
            {idstr}
        ORDER BY
            {FIELD_MAP["TIMESTAMP"]};        
    """

    # Run the query and collect the result
    query_job = bq_client.query(q)
    rows = query_job.result()
    
    # break on empty iterator
    if rows.total_rows == 0:
        raise NoDataError("No data returned", status_code=204)
        
    # Convert Response object (generator) to list-of-dicts
    data = [dict(r) for r in rows]

    # Clean data and apply correction factors
    data = utils.tuneData(data, fields)

    # Apply correction factors to data
    return data


def _requestDataInRadius(srcs, fields, start, end, radius, center, id_ls=None):
    """
    Function to query a field (like Temperature, Humidity, PM, etc.) 
    or list of fields, in date range [start, end], inside a given
    radius. The radius is in kilometers and center is the (Lat,Lon) 
    center of the circle.
    Can include an ID or a list of IDs.
    """
    bbox = utils.convertRadiusToBBox(radius, center)
    data = _requestData(srcs, fields, start, end, bbox=bbox, id_ls=id_ls)
    data = utils.bboxDataToRadiusData(data, radius, center)
    
    if len(data) == 0:
        raise NoDataError("No data returned", status_code=204)

    return data


#http://localhost:8080/api/getEstimateMap?lat_lo=40.644519&lon_lo=-111.971465&lat_hi=40.806852&lon_hi=-111.811118&lat_size=3&lon_size=3&date=2020-10-10T00:00:00Z
# @app.route("/getEstimateMap", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
# @admin_utils.ingroup('admin')
# @limiter.limit('1/minute')
# def getEstimateMap():
#     """
#     lat_hi
#     lat_lo
#     lon_hi
#     lon_lo
#     lat_size
#     lon_size
#     date
    
#     """
#     # this species grid positions should be interpolated in UTM coordinates
#     if "UTM" in request.args:
#         UTM = True
#     else:
#         UTM = False

#     # Get the arguments from the query string
#     if not UTM:
#         try:
#             lat_hi = float(request.args.get('lat_hi'))
#             lat_lo = float(request.args.get('lat_lo'))
#             lon_hi = float(request.args.get('lon_hi'))
#             lon_lo = float(request.args.get('lon_lo'))
#         except ValueError:
#             return 'lat, lon, lat_res, be floats in the lat-lon (not UTM) case', 400
#         try:
#             lat_size = int(request.args.get('lat_size'))
#             lon_size = int(request.args.get('lon_size'))
#         except ValueError:
#             return 'lat, lon, sizes must be ints (not UTM) case', 400

#         lat_res = (lat_hi-lat_lo)/float(lat_size)
#         lon_res = (lon_hi-lon_lo)/float(lon_size)

#     query_date = request.args.get('date')
#     if not utils.validateDate(query_date):
#         msg = f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
#         return msg, 400

#     query_datetime = utils.parseDateString(query_date)

#     print((
#         f"Query parameters: lat_lo={lat_lo} lat_hi={lat_hi}  lon_lo={lon_lo} lon_hi={lon_hi} lat_res={lat_res} lon_res={lon_res} date={query_datetime}"
#     ))

#     # step 0, load up the bounding box from file and check that request is within it
#     bounding_box_vertices = utils.loadBoundingBox()
#     print(f'Loaded {len(bounding_box_vertices)} bounding box vertices.')

#     if not (
#         utils.isQueryInBoundingBox(bounding_box_vertices, lat_lo, lon_lo) and
#         utils.isQueryInBoundingBox(bounding_box_vertices, lat_lo, lon_hi) and
#         utils.isQueryInBoundingBox(bounding_box_vertices, lat_hi, lon_hi) and
#         utils.isQueryInBoundingBox(bounding_box_vertices, lat_hi, lon_lo)):
#         return 'One of the query locations is outside of the bounding box for the database', 400

#     # step 1, load up correction factors from file
#     correction_factors = utils.loadCorrectionFactors()
#     print(f'Loaded {len(correction_factors)} correction factors.')

#     # step 2, load up length scales from file
#     length_scales = utils.loadLengthScales()
#     print(f'Loaded {len(length_scales)} length scales.')

#     print('Loaded length scales:', length_scales, '\n')
#     length_scales = utils.getScalesInTimeRange(length_scales, query_datetime, query_datetime)
#     if len(length_scales) < 1:
#         msg = (
#             f"Incorrect number of length scales({len(length_scales)}) "
#             f"found in between {query_datetime}-1day and {query_datetime}+1day"
#         )
#         return msg, 400

#     latlon_length_scale = length_scales[0]['latlon']
#     elevation_length_scale = length_scales[0]['elevation']
#     time_length_scale = length_scales[0]['time']

#     print(
#         f'Using length scales: latlon={latlon_length_scale} elevation={elevation_length_scale} time={time_length_scale}'
#     )

#     # step 3, query relevent data
#     # for this compute a circle center at the query volume.  Radius is related to lenth scale + the size fo the box.
#     lat = (lat_lo + lat_hi)/2.0
#     lon = (lon_lo + lon_hi)/2.0
#     #    NUM_METERS_IN_MILE = 1609.34
#     #    radius = latlon_length_scale / NUM_METERS_IN_MILE  # convert meters to miles for db query

#     UTM_N_hi, UTM_E_hi, zone_num_hi, zone_let_hi = utils.latlonToUTM(lat_hi, lon_hi)
#     UTM_N_lo, UTM_E_lo, zone_num_lo, zone_let_lo = utils.latlonToUTM(lat_lo, lon_lo)
#     # compute the length of the diagonal of the lat-lon box.  This units here are **meters**
#     lat_diff = UTM_N_hi - UTM_N_lo
#     lon_diff = UTM_E_hi - UTM_E_lo
#     print(f'SPACE_KERNEL_FACTOR_PADDING= {type(SPACE_KERNEL_FACTOR_PADDING)}\nlatlon_length_scale={type(latlon_length_scale)}\nlat_diff={type(lat_diff)}\nlon_diff={type(lon_diff)}')
#     radius = SPACE_KERNEL_FACTOR_PADDING*latlon_length_scale + np.sqrt(lat_diff**2 + lon_diff**2)/2.0

#     if not ((zone_num_lo == zone_num_hi) and (zone_let_lo == zone_let_hi)):
#         return 'Requested region spans UTM zones', 400        


#     #    radius = latlon_length_scale / 70000 + box_diag/2.0
#     # sensor_data = request_model_data_local(
#     #     lats=lat,
#     #     lons=lon,
#     #     radius=radius,
#     #     start_date=query_datetime - TIME_KERNEL_FACTOR_PADDING*timedelta(hours=time_length_scale),
#     #     end_date=query_datetime + TIME_KERNEL_FACTOR_PADDING*timedelta(hours=time_length_scale))
    
#     # Convert dates to strings
#     start = query_datetime - TIME_KERNEL_FACTOR_PADDING * timedelta(hours=time_length_scale)
#     start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
#     end = query_datetime + TIME_KERNEL_FACTOR_PADDING * timedelta(hours=time_length_scale)
#     end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")

#     print(f'Start: {start_str}\nEnd: {end_str}')

#     sensor_data = _requestFieldInRadius(
#         src_tbl=SRC_MAP['SLC'],
#         field='PM2_5',
#         lat=lat,
#         lon=lon,
#         radius=radius,
#         start=start_str,
#         end=end_str
#     )

#     # If it's a tuple then it returned a response code
#     # TODO: Fix this, it is so weird. 
#     if isinstance(sensor_data, tuple):
#         return sensor_data

#     # print("sensor data:", sensor_data)
#     unique_sensors = {datum['DeviceID'] for datum in sensor_data}
#     print(f'Loaded {len(sensor_data)} data points for {len(unique_sensors)} unique devices from bgquery.')

#     # step 3.5, convert lat/lon to UTM coordinates
#     try:
#         utils.convertLatLonToUTM(sensor_data)
#     except ValueError as err:
#         return f'{str(err)}', 400

#     # # Step 4, parse sensor type from the version
#     # sensor_source_to_type = {'AirU': '3003', 'PurpleAir': '5003', 'DAQ': '0000'}
#     # for datum in sensor_data:
#     #     datum['type'] = sensor_source_to_type[datum['SensorSource']]

#     # print(f'Fields: {sensor_data[0].keys()}')

#     # step 4.5, Data Screening
#     print('Screening data')
#     sensor_data = utils.removeInvalidSensors(sensor_data)

#     # Correction factors applied automatically in _requestFieldInRadius()
#     #     # step 5, apply correction factors to the data
#     # for datum in sensor_data:
#     #     datum['PM2_5'] = utils.applyCorrectionFactor(correction_factors, datum['time'], datum['PM2_5'], datum['type'])

#     # step 6, add elevation values to the data
#     for datum in sensor_data:
#         if 'Elevation' not in datum:
#             datum['Elevation'] = elevation_interpolator([datum['Longitude']],[datum['Latitude']])[0]

#     # step 7, Create Model
#     model, time_offset = gaussian_model_utils.createModel(
#         sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale)

    
#     # step 8, build the grid of query locations
#     if not UTM:

#         # TODO: Create function interpolateQueryLocations()
#         #   Tom Made one that might be correct
#         lon_vector, lat_vector = utils.interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_size, lon_size)
#     #        locations_UTM = utm.from_latlon(query_locations_latlon)
#     else:
#         # step 7.5, convert query box to UTM -- do the two far corners and hope for the best
#     #        lat_lo_UTM, lon_lo_UTM, zone_num_lo, zone_let_lo = utils.latlonToUTM(lat_lo, lon_lo)
#     #        lat_hi_UTM, lon_hi_UTM, zone_num_hi, zone_let_hi = utils.latlonToUTM(lat_hi, lon_hi)
#     #        query_locations_UTM = utils.interpolateQueryLocations(lat_lo_UTM, lat_hi_UTM, lon_lo_UTM, lon_hi_UTM, spatial_res)
#     #        query_locations_
#         return 'UTM not yet supported', 400

    
#     #######################
#     # Ross was here
#     #######################

#     # locations_lat = locations_lat.flatten()
#     # locations_lon = locations_lon.flatten()
#     #    print(locations_lat.shape)
#     #    print(locations_lon.shape)
#     elevations = elevation_interpolator(lon_vector, lat_vector)
#     print(elevations.shape)

#     locations_lon, locations_lat = np.meshgrid(lon_vector, lat_vector)
#     # print("B")
#     # print(locations_lat)
#     # print(locations_lon)

#     locations_lat = locations_lat.flatten()
#     locations_lon = locations_lon.flatten()
#     elevations = elevations.flatten()
#     # print("C")
#     # print(locations_lat)
#     # print(locations_lon)

#     # print("D")
#     # print(locations_lat.reshape((lat_size, lon_size)))
#     # print(locations_lon.reshape((lat_size, lon_size)))


#     yPred, yVar = gaussian_model_utils.estimateUsingModel(
#         model, locations_lat, locations_lon, elevations, [query_datetime], time_offset)

#     elevations = (elevations.reshape((lat_size, lon_size))).tolist()
#     yPred = yPred.reshape((lat_size, lon_size))
#     yVar = yVar.reshape((lat_size, lon_size))
#     estimates = yPred.tolist()
#     variances = yVar.tolist()

#     return jsonify({
#             "Elevations": elevations, 
#             "PM2.5": estimates, 
#             "PM2.5 variance": variances, 
#             "Latitudes": lat_vector.tolist(), 
#             "Longitudes": lon_vector.tolist()
#         })


# @app.route("/requestFieldInRadius", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
# def requestFieldInRadius():

#     req_args = [
#         'src', 
#         'field', 
#         'lat', 
#         'lon', 
#         'radius', 
#         'start', 
#         'end'
#     ]

#     r = utils.checkArgs(request.args, req_args)
#     if r[1] != 200: 
#         return r

#     src = request.args.get('src', type=str)
#     field = request.args.get('field', type=str)
#     lat = request.args.get('lat', type=float)
#     lon = request.args.get('lon', type=float)
#     radius = request.args.get('radius', type=float)
#     start = request.args.get('start', type=str)
#     end = request.args.get('end', type=str)

#     # Check source is valid
#     if src not in SRC_MAP:
#         return jsonify({'Error': f"argument 'src' must be included from {', '.join(list(SRC_MAP.keys()))}"}), 400
#     src_tbl = SRC_MAP[request.args.get('src').upper()]

#     # Check field is valid
#     if field not in VALID_QUERY_FIELDS:
#         msg = "Parameter <code>field</code> is invalid. Must be one of the following:<ul>"
#         msg += ''.join([f"<li><code>{f}</code></li>" for f in VALID_QUERY_FIELDS])
#         msg += '</ul><br>Furthermore, only a single field at a time is supported.'
#         return msg, 400
    
#     # Check lat/lon valid
#     if not utils.validLatLon(lat, lon):
#         msg = "Parameter <code>lat</code> or <code>lon</code> is invalid. [-90 < lat < 90], [-180 < lon < 180]"
#         return msg, 400

#     # Check radius valid
#     if not utils.validRadius(radius):
#         msg = "Parameter <code>radius</code> must be a positive float, in meters."
#         return msg, 400

#     # Check that datetimes are valid
#     if not utils.validateDate(start) or not utils.validateDate(end):
#         msg = f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
#         return msg, 400

#     # Perform query and post-query cleanup
#     response = _requestFieldInRadius(src_tbl, field, lat, lon, radius, start, end)
#     if response[1] != 200:
#         return response 
#     else:
#         data = response[0] 

#     return data, 200


# def _requestFieldInRadius(src_tbl, field, lat, lon, radius, start, end):
#     """Function to query a field (like Temperature, Humidity, PM, etc.) 
#     in a radius (kilometers) around GPS (lat,lon) in date range [start, end]"""

#     # Build the query
#     QUERY = f"""
#         CREATE TEMP FUNCTION toRad(x FLOAT64) AS ((x * ACOS(-1)) / 180);
#         SELECT
#             {FIELD_MAP['FIELD_TS']} AS Timestamp,
#             {FIELD_MAP['FIELD_ID']} AS DeviceID,
#             {FIELD_MAP['FIELD_LAT']} AS Latitude,
#             {FIELD_MAP['FIELD_LON']} AS Longitude,
#             {FIELD_MAP['FIELD_ELE']} AS Elevation,
#             {field}
#         FROM 
#             `{src_tbl}`
#         WHERE
#             Timestamp >= @start
#             AND 
#             Timestamp <= @end
#             AND
#             ACOS(
#                 SIN( toRad(@lat) ) * 
#                 SIN( toRad(`{FIELD_MAP['FIELD_LAT']}`) ) + 
#                 COS( toRad(@lat) ) * 
#                 COS( toRad(`{FIELD_MAP['FIELD_LAT']}`) ) * 
#                 COS( toRad(`{FIELD_MAP['FIELD_LON']}`) - toRad(@lon) )
#             ) * 6371 <= @radius
#         ORDER BY 
#             `Timestamp`;        
#     """

#     job_config = QueryJobConfig(
#         query_parameters=[
#             ScalarQueryParameter("lat", "NUMERIC", lat),
#             ScalarQueryParameter("lon", "NUMERIC", lon),
#             ScalarQueryParameter("radius", "NUMERIC", radius),
#             ScalarQueryParameter("start", "TIMESTAMP", start),
#             ScalarQueryParameter("end", "TIMESTAMP", end),
#         ]
#     )

#     print(QUERY)

#     # Run the query and collect the result
#     query_job = bq_client.query(QUERY, job_config=job_config)

#     if query_job.error_result:
#         print(query_job.error_result)

#         # TODO: Is it okay to dump this info? 
#         return f"Invalid API call - check documentation.<br>{query_job.error_result}", 400

#     # Wait for query to finish
#     rows = query_job.result()

#     # Format as proper dicts
#     model_data = []
    
#     # break on empty iterator
#     if rows.total_rows == 0:
#         msg = "No data returned"
#         return msg, 200

#     for row in rows:
#         model_data.append({
#             "DeviceID": str(row.DeviceID),
#             "Latitude": row.Latitude,
#             "Longitude": row.Longitude,
#             "Timestamp": row.Timestamp,
#             f"{field}": row[field]
#         })

#     # Apply correction factors to data
#     return jsonify(utils.applyCorrectionFactorsToList(model_data)), 200


# # Gets data within radius of the provided lat lon within the time frame. The radius units are latlon degrees so this is an approximate bounding circle
# def request_model_data_local(lat, lon, radius, start_date, end_date):
#     model_data = []
#     # get the latest sensor data from each sensor
#     query = f"""
#         SELECT 
#             ID, time, PM2_5, Latitude, Longitude
#         FROM 
#             `{BIGQUERY_TABLE_SLC}`
#         WHERE 
#             time > @start_date AND time < @end_date
#             AND
#             SQRT(POW(Latitude - @lat, 2) + POW(Longitude - @lon, 2)) <= @radius
#         ORDER BY time ASC
#     """

#     job_config = QueryJobConfig(
#         query_parameters=[
#             ScalarQueryParameter("lat", "NUMERIC", lat),
#             ScalarQueryParameter("lon", "NUMERIC", lon),
#             ScalarQueryParameter("radius", "NUMERIC", radius),
#             ScalarQueryParameter("start_date", "TIMESTAMP", utils.datetimeToBigQueryTimestamp(start_date)),
#             ScalarQueryParameter("end_date", "TIMESTAMP", utils.datetimeToBigQueryTimestamp(end_date)),
#         ]
#     )

#     query_job = bq_client.query(query, job_config=job_config)

#     if query_job.error_result:
#         print(query_job.error_result)
#         return "Invalid API call - check documentation.", 400
#     rows = query_job.result()  # Waits for query to finish

#     for row in rows:
#         model_data.append({
#             "ID": str(row.ID),
#             "Latitude": row.Latitude,
#             "Longitude": row.Longitude,
#             "time": row.time,
#             "PM2_5": row.PM2_5,
#             "SensorModel": row.SensorModel,
#             "SensorSource": row.SensorSource,
#         })

#     # Apply correction factors
#     model_data = utils.applyCorrectionFactorsToList(model_data)
#     # correction_factors = utils.loadCorrectionFactors(getenv("CORRECTION_FACTORS_FILENAME"))
#     # print(f'Loaded {len(correction_factors)} correction factors.')

#     # for datum in sensor_data:
#     #     datum['PM2_5'] = utils.applyCorrectionFactor(correction_factors, datum['time'], datum['PM2_5'], datum['type'])

#     return model_data


# Gets data within radius of the provided lat lon within the time frame. The radius units are latlon degrees so this is an approximate bounding circle
# @app.route("/api/request_model_data/", methods=['GET'])
# def request_model_data():
#     query_parameters = request.args
#     lat = query_parameters.get('lat')
#     lon = query_parameters.get('lon')
#     radius = query_parameters.get('radius')
#     query_start_date = request.args.get('start_date')
#     query_end_date = request.args.get('end_date')
#     if not utils.validateDate(query_start_date) or not utils.validateDate(query_end_date):
#         resp = jsonify({'message': f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"})
#         return resp, 400

#     query_start_datetime = utils.parseDateString(query_start_date)
#     query_end_datetime = utils.parseDateString(query_end_date)
#     model_data = request_model_data_local(lat, lon, radius, query_start_datetime, query_end_datetime)
#     return jsonify(model_data)


# @app.route("/api/getPredictionsForLocation/", methods=['GET'])
# def getPredictionsForLocation():
#     # Check that the arguments we want exist
#     # if not validateInputs(['lat', 'lon', 'predictionsperhour', 'start_date', 'end_date'], request.args):
#     #     return 'Query string is missing one or more of lat, lon, predictionsperhour, start_date, end_date', 400

#     # step -1, parse query parameters
#     try:
#         query_lat = float(request.args.get('lat'))
#         query_lon = float(request.args.get('lon'))
#         query_period = float(request.args.get('predictionsperhour'))
#     except ValueError:
#         return 'lat, lon, predictionsperhour must be floats.', 400

#     query_start_date = request.args.get('start_date')
#     query_end_date = request.args.get('end_date')

#     # Check that the data is formatted correctly
#     if not utils.validateDate(query_start_date) or not utils.validateDate(query_end_date):
#         msg = f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
#         return msg, 400

#     query_start_datetime = utils.parseDateString(query_start_date)
#     query_end_datetime = utils.parseDateString(query_end_date)

#     print((
#         f"Query parameters: lat={query_lat} lon={query_lon} start_date={query_start_datetime}"
#         f" end_date={query_end_datetime} predictionsperhour={query_period}"
#     ))

#     # step 0, load up the bounding box from file and check that request is within it
#     bounding_box_vertices = utils.loadBoundingBox()
#     print(f'Loaded {len(bounding_box_vertices)} bounding box vertices.')

#     if not utils.isQueryInBoundingBox(bounding_box_vertices, query_lat, query_lon):
#         return 'The query location is outside of the bounding box.', 400

#     # step 1, load up correction factors from file
#     correction_factors = utils.loadCorrectionFactors()
#     print(f'Loaded {len(correction_factors)} correction factors.')

#     # step 2, load up length scales from file
#     length_scales = utils.loadLengthScales()
#     print(f'Loaded {len(length_scales)} length scales.')

#     print('Loaded length scales:', length_scales, '\n')
#     length_scales = utils.getScalesInTimeRange(length_scales, query_start_datetime, query_end_datetime)
#     if len(length_scales) < 1:
#         msg = (
#             f"Incorrect number of length scales({len(length_scales)}) "
#             f"found in between {query_start_datetime} and {query_end_datetime}"
#         )
#         return msg, 400

#     latlon_length_scale = length_scales[0]['latlon']
#     elevation_length_scale = length_scales[0]['elevation']
#     time_length_scale = length_scales[0]['time']

#     print(
#         f'Using length scales: latlon={latlon_length_scale} elevation={elevation_length_scale} time={time_length_scale}'
#     )

#     # step 3, query relevent data
#     APPROX_METERS_PER_LATLON_DEGREE_IN_UTAH = 70000
#     radius = latlon_length_scale / APPROX_METERS_PER_LATLON_DEGREE_IN_UTAH  # convert meters to latlon degrees for db query
#     sensor_data = request_model_data_local(
#         lat=query_lat,
#         lon=query_lon,
#         radius=radius,
#         start_date=query_start_datetime - timedelta(hours=time_length_scale),
#         end_date=query_end_datetime + timedelta(hours=time_length_scale))

#     unique_sensors = {datum['ID'] for datum in sensor_data}
#     print(f'Loaded {len(sensor_data)} data points for {len(unique_sensors)} unique devices from bgquery.')

#     # step 3.5, convert lat/lon to UTM coordinates
#     try:
#         utils.convertLatLonToUTM(sensor_data)
#     except ValueError as err:
#         return f'{str(err)}', 400

#     sensor_data = [datum for datum in sensor_data if datum['zone_num'] == 12]

#     unique_sensors = {datum['ID'] for datum in sensor_data}
#     print((
#         "After removing points with zone num != 12: "
#         f"{len(sensor_data)} data points for {len(unique_sensors)} unique devices."
#     ))

#     # Step 4, parse sensor type from the version
#     sensor_source_to_type = {'AirU': '3003', 'PurpleAir': '5003', 'DAQ': '5003'}
#     for datum in sensor_data:
#         datum['type'] = sensor_source_to_type[datum['SensorSource']]

#     print(f'Fields: {sensor_data[0].keys()}')

#     # step 4.5, Data Screening
#     print('Screening data')
#     sensor_data = utils.removeInvalidSensors(sensor_data)

#     # step 5, apply correction factors to the data
#     for datum in sensor_data:
#         datum['PM2_5'] = utils.applyCorrectionFactor(correction_factors, datum['time'], datum['PM2_5'], datum['type'])

#     # step 6, add elevation values to the data
#     for datum in sensor_data:
#         if 'Altitude' not in datum:
#             datum['Altitude'] = elevation_interpolator([datum['Latitude']], [datum['Longitude']])[0]

#     # step 7, Create Model
#     model, time_offset = gaussian_model_utils.createModel(
#         sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale)

#     # step 8, get predictions from model
#     query_dates = utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_period)
#     query_elevation = elevation_interpolator([query_lat], [query_lon])[0]
#     predictions = gaussian_model_utils.predictUsingModel(
#         model, query_lat, query_lon, query_elevation, query_dates, time_offset)

#     return jsonify(predictions)

######################  Testing this
### this allows multi lats/lons to be specified.  
# @app.route("/api/getEstimatesForLocations/", methods=['GET'])
# def getEstimatesForLocations():
#     # Check that the arguments we want exist
#     # if not validateInputs(['lat', 'lon', 'estimatesperhour', 'start_date', 'end_date'], request.args):
#     #     return 'Query string is missing one or more of lat, lon, estimatesperhour, start_date, end_date', 400

#     # step -1, parse query parameters
#     try:
#         query_rate = float(request.args.get('estimaterate'))
#     except ValueError:
#         return 'Estimates must be floats.', 400

# ## regular expression for floats
#     regex = r'[+-]?[0-9]+\.[0-9]+'
#     query_lats = np.array(re.findall(regex,request.args.get('lat'))).astype(np.float)
#     query_lons = np.array(re.findall(regex,request.args.get('lon'))).astype(np.float)
#     if (query_lats.shape != query_lons.shape):
#         return 'lat, lon must be equal sized arrays of floats:'+str(query_lats)+' ; ' + str(query_lons), 400

#     num_locations = query_lats.shape[0]

#     query_start_date = request.args.get('start_date')
#     query_end_date = request.args.get('end_date')

#     # Check that the data is formatted correctly
#     if not utils.validateDate(query_start_date) or not utils.validateDate(query_end_date):
#         msg = f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
#         return msg, 400

#     query_start_datetime = utils.parseDateString(query_start_date)
#     query_end_datetime = utils.parseDateString(query_end_date)

#     print((
#         f"Query parameters: lat={query_lats} lon={query_lons} start_date={query_start_datetime}"
#         f" end_date={query_end_datetime} estimaterate={query_rate}"
#     ))

#     # step 0, load up the bounding box from file and check that request is within it
#     bounding_box_vertices = utils.loadBoundingBox()
#     print(f'Loaded {len(bounding_box_vertices)} bounding box vertices.')

#     for i in range(num_locations):
#         if not utils.isQueryInBoundingBox(bounding_box_vertices, query_lats[i], query_lons[i]):
#             return 'The query location, {query_lats[i]},{query_lons[i]},  is outside of the bounding box.', 400

#     # step 1, load up correction factors from file
#     # correction_factors = utils.loadCorrectionFactors(getenv("CORRECTION_FACTORS_FILENAME"))
#     # print(f'Loaded {len(correction_factors)} correction factors.')

#     # step 2, load up length scales from file
#     length_scales = utils.loadLengthScales()
#     print(f'Loaded {len(length_scales)} length scales.')

#     print('Loaded length scales:', length_scales, '\n')
#     length_scales = utils.getScalesInTimeRange(length_scales, query_start_datetime, query_end_datetime)
#     if len(length_scales) < 1:
#         msg = (
#             f"Incorrect number of length scales({len(length_scales)}) "
#             f"found in between {query_start_datetime} and {query_end_datetime}"
#         )
#         return msg, 400

#     latlon_length_scale = length_scales[0]['latlon']
#     elevation_length_scale = length_scales[0]['elevation']
#     time_length_scale = length_scales[0]['time']

#     print(
#         f'Using length scales: latlon={latlon_length_scale} elevation={elevation_length_scale} time={time_length_scale}'
#     )

#     # step 3, query relevent data

# # these conversions were when we were messing around with specifying radius in miles and so forth.      
# #    NUM_METERS_IN_MILE = 1609.34
# #    radius = latlon_length_scale / NUM_METERS_IN_MILE  # convert meters to miles for db query

# #    radius = latlon_length_scale / 70000


# # radius is in meters, as is the length scale and UTM.    
#     radius = SPACE_KERNEL_FACTOR_PADDING*latlon_length_scale

#     # sensor_data = request_model_data_local(
#     #     query_lats,
#     #     query_lons,
#     #     radius=radius,
#     #     start_date=query_start_datetime - timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale),
#     #     end_date=query_end_datetime + timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale))

#     # sensor_data = _requestFieldInRadius()

#     unique_sensors = {datum['ID'] for datum in sensor_data}
#     print(f'Loaded {len(sensor_data)} data points for {len(unique_sensors)} unique devices from bgquery.')

#     # step 3.5, convert lat/lon to UTM coordinates
#     try:
#         utils.convertLatLonToUTM(sensor_data)
#     except ValueError as err:
#         return f'{str(err)}', 400

#     sensor_data = [datum for datum in sensor_data if datum['zone_num'] == 12]

#     unique_sensors = {datum['ID'] for datum in sensor_data}
#     print("After removing points with zone num != 12: " + f"{len(sensor_data)} data points for {len(unique_sensors)} unique devices.")

#     # Step 4, parse sensor type from the version
#     sensor_source_to_type = {'AirU': '3003', 'PurpleAir': '5003', 'DAQ': '0000'}
# # DAQ does not need a correction factor
#     for datum in sensor_data:
#         datum['type'] =  sensor_source_to_type[datum['SensorSource']]

#     print(f'Fields: {sensor_data[0].keys()}')

#     # step 4.5, Data Screening
#     print('Screening data')
#     sensor_data = utils.removeInvalidSensors(sensor_data)

#     # # step 5, apply correction factors to the data
#     # for datum in sensor_data:
#     #     datum['PM2_5'] = utils.applyCorrectionFactor(correction_factors, datum['time'], datum['PM2_5'], datum['type'])

#     # step 6, add elevation values to the data
#     for datum in sensor_data:
#         if 'Altitude' not in datum:
#             datum['Altitude'] = elevation_interpolator([datum['Longitude']],[datum['Latitude']])[0]

#     # step 7, Create Model
#     model, time_offset = gaussian_model_utils.createModel(
#         sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale)

#     # step 8, get estimates from model
#     query_dates = utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_rate)

#     # note - the elevation grid is the wrong way around, so you need to put in lons first
#     query_elevations = elevation_interpolator(query_lons, query_lats)
#     yPred, yVar = gaussian_model_utils.estimateUsingModel(
#         model, query_lats, query_lons, query_elevations, query_dates, time_offset)

#     num_times = len(query_dates)
#     estimates = []

#     for i in range(num_times):
#         estimates.append(
#             {'PM2_5': (yPred[:,i]).tolist(), 'variance': (yVar[:,i]).tolist(), 'datetime': query_dates[i].strftime('%Y-%m-%d %H:%M:%S%z'), 'Latitude': query_lats.tolist(), 'Longitude': query_lons.tolist(), 'Elevation': query_elevations.tolist()}
#             )

#     return jsonify(estimates)

# ***********************************************************
# Function: request_data_flask(d) - used to populate the map with sensors
# Called by slider.js
# Parameter:
# Return: Last recorded sensor input from all sensors in the DB
# ***********************************************************
# @app.route("/request_data_flask", methods=['GET'])
# def request_data_flask(d):
#     sensor_list = []
 
#     logging.error(request.view_args)
#     logging.error(request.args)
#     logging.error(vars(request.args))
#     req_args = [
#         'src'
#         ]

#     r = utils.checkArgs(request.args, req_args)
#     if r[1] != 200: 
#         return r

#     src = request.args.get('src')
#     if src not in SRC_MAP:
#         return jsonify({'Error': f"argument 'src' must be included from {', '.join(list(SRC_MAP.keys()))}"}), 400
#     sensors_table = SRC_MAP[request.args.get('src').upper()]

#     # get the latest sensor data from each sensor
#     q = ("SELECT `" + sensor_table + "`.DEVICEID, PM2_5, Latitude, Longitude, Timestamp "
#          "FROM `" + sensor_table + "` "
#          "INNER JOIN (SELECT DEVICEID, MAX(Timestamp) as maxts "
#          "FROM `" + sensor_table + "` GROUP BY DEVICEID) mr "
#          "ON `" + sensor_table + "`.DEVICEID = mr.DEVICEID AND Timestamp = maxts WHERE Timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY);")

#     query_job = bq_client.query(q)
#     rows = query_job.result()  # Waits for query to finish
#     for row in rows:
#         sensor_list.append({"DEVICEID": str(row.DEVICEID),
#                             "Latitude": row.Latitude,
#                             "Longitude": row.Longitude,
# 			    "Timestamp": row.Timestamp,
#                             "PM2_5": row.PM2_5})

    

#     return jsonify(sensor_list), 200



# ***********************************************************
# Function: request_historical(d)
# Called by historical_data.js
# Parameter: JSON format containing DEVICE_ID, DAYS (number of days to query ), and CITY ( to be discussed )
# Return: JSON sensor data for the given sensor and given number of days
# ***********************************************************
# @app.route("/request_historical/<d>/", methods=['GET'])
# def request_historical(d):
#     sensor_list = []
#     json_data = json.loads(d)
#     id = json_data["DEVICEID"]
#     days = str(json_data["DAYS"])
#     sensor_table = "tetrad-296715.telemetry." + json_data["CITY"]
#     # get the latest sensor data from each sensor
#     q = ("SELECT DATETIME_TRUNC(DATETIME(Timestamp, 'America/Denver'), HOUR) AS D, ROUND(AVG(PM2_5),2) AS PM2_5 "
#         "FROM `" + sensor_table + "` "
#         "WHERE DEVICEID=\'" + id + "\' "
#         "AND Timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL " + days + " DAY) "
#         "GROUP BY D ORDER BY D;")

#     query_job = bq_client.query(q)
#     rows = query_job.result()  # Waits for query to finish
#     for row in rows:
#         sensor_list.append({"D_TIME": row.D,
#                             "PM2_5": row.PM2_5})

#     return jsonify(sensor_list), 200
