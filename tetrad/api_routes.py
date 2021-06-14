from datetime import datetime, timedelta
import pytz
from flask import request, jsonify
import functools
from google.cloud.bigquery import Client as BQClient, QueryJobConfig, ScalarQueryParameter
from tetrad import app, cache, admin_utils, limiter, utils
from tetrad.api_consts import *
from tetrad.classes import ArgumentError, NoDataError
# from tetrad import gaussian_model_utils
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


bq_client = BQClient()

# https://api.tetradsensors.com/liveSensors?src=all&field=pm2_5
@app.route("/liveSensors", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
# @app.route("/liveSensors", methods=["GET"])
# @cache.cached(timeout=119)
def liveSensors():

    def argParseDelta(delta):
        delta = delta or 15
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
    Q_LABELS = utils.queryBuildLabels(srcs)
    # Build the query
    SUBQ = f"""SELECT 
                    {Q_FIELDS}
                FROM 
                    `{BQ_PATH_TELEMETRY}`
                WHERE 
                    {Q_LABELS}
                        AND
                    {FIELD_MAP["TIMESTAMP"]} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {delta} MINUTE)
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
                        {FIELD_MAP["DEVICEID"]} 
                    ORDER BY 
                        {FIELD_MAP["TIMESTAMP"]} DESC
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
    data = utils.tuneAllFields(data, fields)

    return jsonify(data), 200

# https://api.tetradsensors.com/requestData?src=slc_ut&field=pm2_5&start=2021-01-01T00:00:00Z&end=2021-01-22T00:00:00Z
@app.route("/requestData", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
# @app.route("/requestData", methods=["GET"])
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
        'field', 
        'start', 
        'end',
    ]
    # You don't have to include 'src' if 'device' is here
    if not request.args.get('device', type=str):
        req_args.append('src')

    try:
        utils.verifyArgs(request.args, req_args, args)
        
        # Required
        srcs    = utils.argParseSources(request.args.get('src', type=str), canBeNone=True)
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
        data = _requestData(srcs, fields, start, end, radius=rc[0], center=rc[1], id_ls=devices)
    else:
        data = _requestData(srcs, fields, start, end, bbox=box, id_ls=devices)

    if isinstance(data, int):
        if data == 408:
            return "Timeout (2 minutes). Try a smaller query.", 408
        else:
            return "Something went wrong. That's all we know. Contact the developers.", data

    response = jsonify(data)
    response.status_code = 200
    return response


def _requestData(srcs, fields, start, end, bbox=None, radius=None, center=None, id_ls=None, removeNulls=False):
    """
    Function to query a field (like Temperature, Humidity, PM, etc.) 
    or list of fields, in date range [start, end], inside a bounding
    box. The bounding box is a dict {'lat_hi', 'lat_lo', 'lon_hi', 'lon_lo'} 
    coordinates.
    If radius, radius is in meters, center is dict {'lat', 'lon'}
    Can include an ID or a list of IDs
    """

    if id_ls:
        query_devs = utils.idsToWHEREClause(id_ls, FIELD_MAP['DEVICEID'])
    else:
        query_devs = "True"

    query_fields = utils.queryBuildFields(fields)

    if bbox:
        n, s, e, w = bbox['lat_hi'], bbox['lat_lo'], bbox['lon_hi'], bbox['lon_lo']
        query_region = f"""
            ST_WITHIN(
                {FIELD_MAP["GPS"]}, 
                ST_GeogFromGeoJSON(
                    '{{"type": "Polygon", "coordinates": [[[{e},{n}],[{e},{s}],[{w},{s}],[{w},{n}],[{e},{n}]]]}}'
                )
            )
        """
    elif radius:
        query_region = f"""
            ST_DWITHIN(
                {FIELD_MAP["GPS"]}, 
                ST_GEOGPOINT({center['lon']}, {center['lat']}),
                {radius * 1000}
            )
        """
    else:
        query_region = "True"

    query_labels = utils.queryBuildLabels(srcs)
    QUERY = f"""
        SELECT 
            {query_fields}
        FROM 
            `{BQ_PATH_TELEMETRY}` 
        WHERE 
            {query_labels}
                AND
            {FIELD_MAP["TIMESTAMP"]} >= "{start}"
                AND
            {FIELD_MAP["TIMESTAMP"]} <= "{end}"
                AND
            {query_region}
                AND
            {query_devs}
        ORDER BY
            {FIELD_MAP["TIMESTAMP"]}
    """

    # data = ' '.join([i for i in QUERY.replace('\n', ' ').split(' ') if i])

    # Run the query and collect the result
    # try:
    query_job = bq_client.query(QUERY)
    rows = query_job.result()   
    # except Exception as e:
    #     print(str(e))
    #     return 408
    
    # # break on empty iterator
    if rows.total_rows == 0:
        raise NoDataError("No data returned.", status_code=222)
        
    # # Convert Response object (generator) to list-of-dicts
    data = [dict(r) for r in rows]

    # # Clean data and apply correction factors
    # data = utils.tuneAllFields(data, fields, removeNulls=removeNulls)

    # Apply correction factors to data
    
    return data


@app.route("/nickname", methods=["GET"], subdomain=getenv('SUBDOMAIN_API'))
# @app.route("/nickname", methods=["GET"])
def nickname():

    args = [
        'device',
        'nickname'
    ]

    req_args = args

    try:
        utils.verifyArgs(request.args, req_args, args)
        device = utils.argParseDevices(request.args.get('device', type=str), single_device=True)

        # nicknames
        nickname = request.args.get('nickname')
        if not re.match(r'^[ -~]{1,128}$', nickname):
            raise ArgumentError(f"Parameter 'nickname' must be be between 1 and 128 ASCII characters.")
        
    except ArgumentError as e:
        raise

    # Perform the UPDATE query
    query = f'''
    UPDATE
        `{PROJECT_ID}.{getenv('BQ_DATASET_META')}.{getenv('BQ_TABLE_META_DEVICES')}`
    SET
        {getenv('FIELD_NN')} = "{nickname}"
    WHERE
        {getenv('FIELD_ID')} = "{device}"
    '''

    print(query)
    bq_client.query(query)

    return 'success', 200
