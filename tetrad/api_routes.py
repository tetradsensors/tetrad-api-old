from datetime import datetime, timedelta
import pytz
from flask import request, jsonify
import functools
from google.cloud.bigquery import Client, QueryJobConfig, ScalarQueryParameter
from tetrad import app, cache, admin_utils, limiter, utils
from tetrad.api_consts import *
from tetrad.classes import ArgumentError, NoDataError
from tetrad import gaussian_model_utils
import json
import numpy as np 
from os import getenv
# import pandas as pd 
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


bq_client = Client(project=PROJECT_ID)


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
    data = utils.tuneAllFields(data, fields)

    return jsonify(data), 200

# https://api.tetradsensors.com/requestData?src=slc_ut&field=pm2_5&start=2021-01-01T00:00:00Z&end=2021-01-22T00:00:00Z
@app.route("/requestData", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
def requestData():
    """
    Arguments:
    @param: qtype   (required)  
    @param: field   (required)
    @param: start   (required)
    @param: end     (required)
    @param: devices (optional)  Single device or list of devices
    @param: box     (optional)  List of coordinates in this order: North, South, East, West
    @param: radius  (optional)  Radius in kilometers
    @param: center  (optional)  Required if 'radius' is supplied. Lat,Lon center of radius. &center=42.012,-111.423&
    """

    args = [
        'src',
        'field',
        'start',
        'end',
        'device',
        'box',
        'radius',
        'center'
    ]

    req_args = [
        'src', 
        'field', 
        'start', 
        'end',
    ]

    try:
        utils.verifyArgs(request.args, req_args, args)
        
        # Required
        srcs    = utils.argParseSources(request.args.get('src', type=str))
        fields  = utils.argParseFields(request.args.get('field', type=str))
        start   = utils.argParseDatetime(request.args.get('start', type=str))
        end     = utils.argParseDatetime(request.args.get('end', type=str))
        
        # Optional
        devices = utils.argParseDevices(request.args.get('device', type=str))
        box     = utils.argParseBBox(request.args.get('box', type=str))
        rc      = utils.argParseRadiusArgs(
                    request.args.get('radius', type=float),
                    request.args.get('center', type=str))
    except ArgumentError as e:
        raise
    except Exception as e:
        logging.error(str(e))
        raise


    #################################
    # Query Picker
    #################################
    data = None
    if box and rc:
        raise ArgumentError("Must choose either 'box' or 'radius','center' arguments", status_code=400)
    elif rc:
        data = _requestDataInRadius(srcs, fields, start, end, radius=rc[0], center=rc[1], id_ls=devices)
    else:
        data = _requestData(srcs, fields, start, end, bbox=box, id_ls=devices)

    response = jsonify(data)
    response.status_code = 200
    return response


def _requestData(srcs, fields, start, end, bbox=None, id_ls=None, removeNulls=False):
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
            {FIELD_MAP["LATITUDE"]}  <= {bbox[0]}
                AND
            {FIELD_MAP["LATITUDE"]}  >= {bbox[1]}
                AND
            {FIELD_MAP["LONGITUDE"]} <= {bbox[2]}
                AND
            {FIELD_MAP["LONGITUDE"]} >= {bbox[3]}
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

    logging.error(q)

    # Run the query and collect the result
    query_job = bq_client.query(q)
    rows = query_job.result()
    
    # break on empty iterator
    if rows.total_rows == 0:
        raise NoDataError("No data returned.", status_code=222)
        
    # Convert Response object (generator) to list-of-dicts
    data = [dict(r) for r in rows]

    # Clean data and apply correction factors
    data = utils.tuneAllFields(data, fields, removeNulls=removeNulls)

    # Apply correction factors to data
    return data


def _requestDataInRadius(srcs, fields, start, end, radius, center, id_ls=None, removeNulls=False):
    """
    Function to query a field (like Temperature, Humidity, PM, etc.) 
    or list of fields, in date range [start, end], inside a given
    radius. The radius is in kilometers and center is the (Lat,Lon) 
    center of the circle.
    Can include an ID or a list of IDs.
    """
    bbox = utils.convertRadiusToBBox(radius, center)
    data = _requestData(srcs, fields, start, end, bbox=bbox, id_ls=id_ls, removeNulls=removeNulls)
    data = utils.bboxDataToRadiusData(data, radius, center)

    
    if len(data) == 0:
        raise NoDataError("No data returned.", status_code=222)

    return data


@app.route("/getEstimateMap", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
# @admin_utils.ingroup('admin')
# @limiter.limit('1/minute')
def getEstimateMap():
    """
    src
    # lat_hi
    # lat_lo
    # lon_hi
    # lon_lo
    lat_size
    lon_size
    date
    
    """
    print("getEstimateMap")
    
    # TOM: Removed everything related to "UTM" source. 
    # # this species grid positions should be interpolated in UTM coordinates
    # if "UTM" in request.args:
    #     UTM = True
    # else:
    #     UTM = False

    # Get the arguments from the query string
    # if not UTM:

    args = [
        'src',
        'lat_size',
        'lon_size',
        'date'
    ]

    req_args = [
        'src', 
        'lat_size',
        'lon_size',
        'date'
    ]

    try:
        utils.verifyArgs(request.args, req_args, args)
        src = utils.argParseSources(request.args.get('src'), single_source=True)
    #     lat_hi = utils.argParseLat(request.args.get('lat_hi', type=float))
    #     lat_lo = utils.argParseLat(request.args.get('lat_lo', type=float))
    #     lon_hi = utils.argParseLon(request.args.get('lon_hi', type=float))
    #     lon_lo = utils.argParseLon(request.args.get('lon_lo', type=float))
        query_datetime = utils.argParseDatetime(request.args.get('date', type=str))
    except ArgumentError:
        raise
    try:
        lat_size = int(request.args.get('lat_size'))
        lon_size = int(request.args.get('lon_size'))
    except ValueError:
        raise ArgumentError('lat, lon, sizes must be ints (not UTM) case', 400)

    ##################################################################
    # STEP 0: Load up the bounding box from file and check 
    #         that request is within it
    ##################################################################

    region = utils.getModelRegion(src)
    if not region:
        raise ArgumentError('src bad', 400)
    
    lat_lo = region['lat_lo']
    lat_hi = region['lat_hi']
    lon_lo = region['lon_lo']
    lon_hi = region['lon_hi']

    print(f'bounding box for {src}:', lat_lo, lat_hi, lon_lo, lon_hi)


    # TOM: replaced bounding box with "model_boxes.json" stored on Google Cloud Storage
    # bounding_box_vertices = utils.loadBoundingBox()
    # print(f'Loaded {len(bounding_box_vertices)} bounding box vertices.')

    # TOM: replaced query bounding box with query source and we now use predefined
    #       bounding boxes for each city. 
    # if not (
    #     utils.isQueryInBoundingBox(bounding_box_vertices, lat_lo, lon_lo) and
    #     utils.isQueryInBoundingBox(bounding_box_vertices, lat_lo, lon_hi) and
    #     utils.isQueryInBoundingBox(bounding_box_vertices, lat_hi, lon_hi) and
    #     utils.isQueryInBoundingBox(bounding_box_vertices, lat_hi, lon_lo)):
    #     raise ArgumentError('One of the query locations is outside of the bounding box for the database', 400)

    ##################################################################
    # STEP 1: Load up length scales from file
    ##################################################################

    length_scales = utils.loadLengthScales()
    length_scales = utils.getScalesInTimeRange(length_scales, query_datetime, query_datetime)
    if len(length_scales) < 1:
        msg = (
            f"Incorrect number of length scales({len(length_scales)}) "
            f"found in between {query_datetime}-1day and {query_datetime}+1day"
        )
        raise ArgumentError(msg, 400)

    latlon_length_scale = length_scales[0]['latlon']
    elevation_length_scale = length_scales[0]['elevation']
    time_length_scale = length_scales[0]['time']


    ##################################################################
    # STEP 2: Query relevant data
    ##################################################################

    # Compute a circle center at the query volume.  Radius is related to lenth scale + the size of the box.
    lat = (lat_lo + lat_hi) / 2.0
    lon = (lon_lo + lon_hi) / 2.0

    UTM_N_hi, UTM_E_hi, zone_num_hi, zone_let_hi = utils.latlonToUTM(lat_hi, lon_hi)
    UTM_N_lo, UTM_E_lo, zone_num_lo, zone_let_lo = utils.latlonToUTM(lat_lo, lon_lo)
    
    # compute the length of the diagonal of the lat-lon box.  This units here are **meters**
    lat_diff = UTM_N_hi - UTM_N_lo
    lon_diff = UTM_E_hi - UTM_E_lo

    radius = SPACE_KERNEL_FACTOR_PADDING * latlon_length_scale + np.sqrt(lat_diff**2 + lon_diff**2) / 2.0

    if not ((zone_num_lo == zone_num_hi) and (zone_let_lo == zone_let_hi)):
        raise ArgumentError('Requested region spans UTM zones', 400)
    

    # Convert dates to strings
    start = query_datetime - (TIME_KERNEL_FACTOR_PADDING * timedelta(hours=time_length_scale))
    end = query_datetime + (TIME_KERNEL_FACTOR_PADDING * timedelta(hours=time_length_scale))
    start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")

    sensor_data = _requestDataInRadius(
        srcs=[src], 
        fields=[getenv("Q_PM2"), getenv("Q_ELE")], 
        start=start_str, 
        end=end_str, 
        radius=radius, 
        center=(lat, lon),
        removeNulls=[getenv("Q_PM2")]
    )

    ##################################################################
    # STEP 3: Convert lat/lon to UTM coordinates
    ##################################################################
    
    try:
        sensor_data = utils.convertLatLonToUTM(sensor_data)
    except ValueError as err:
        return f'Error converting lat/lon to UTM: {str(err)}', 400

    ##################################################################
    # STEP 4: Add elevation values to the data if missing
    ##################################################################
    
    # We need the entire elevation matrix in STEP 6, so load it 
    #   here even if we don't use it in the proceeding loop.
    elevationInterpolator = utils.setupElevationInterpolatorForSource(src)

    # Loop through every row and add elevation from the elevation
    #   interpolator if the row is missing elevation data
    for datum in sensor_data:
        if ('Elevation' not in datum) or (datum['Elevation'] is None):
            print(f"Add elevation for {datum}")
            datum['Elevation'] = elevationInterpolator([datum['Longitude']],[datum['Latitude']])[0]

    ##################################################################
    # STEP 5: Create Model
    ##################################################################
    
    model, time_offset = gaussian_model_utils.createModel(
        sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale)

    ##################################################################
    # STEP 6: Build the grid of query locations
    ##################################################################
    
    # if not UTM:
    lon_vector, lat_vector = utils.interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_size, lon_size)
    # else:
    #     return ArgumentError('UTM not yet supported', 400)

    elevations = elevationInterpolator(lon_vector, lat_vector)
    locations_lon, locations_lat = np.meshgrid(lon_vector, lat_vector)

    locations_lat = locations_lat.flatten()
    locations_lon = locations_lon.flatten()
    elevations = elevations.flatten()

    yPred, yVar = gaussian_model_utils.estimateUsingModel(
        model, locations_lat, locations_lon, elevations, [query_datetime], time_offset)

    elevations = (elevations.reshape((lat_size, lon_size))).tolist()
    yPred = yPred.reshape((lat_size, lon_size))
    yVar = yVar.reshape((lat_size, lon_size))
    estimates = yPred.tolist()
    variances = yVar.tolist()

    response = jsonify({
                "Elevations": elevations, 
                "PM2.5": estimates, 
                "PM2.5 variance": variances, 
                "Latitudes": lat_vector.tolist(), 
                "Longitudes": lon_vector.tolist()
               })  
    response.status_code = 200
    return response
