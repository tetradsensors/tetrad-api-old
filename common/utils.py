from datetime import datetime, timedelta, timezone
import common.gaussian_model_utils
import common.jsonutils
import common.utils
from google.cloud import bigquery
import pytz
import utm
import json
from matplotlib.path import Path
import numpy as np
from scipy import interpolate
from scipy.io import loadmat
import csv
import logging


DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
TIME_KERNEL_FACTOR_PADDING = 3.0
SPACE_KERNEL_FACTOR_PADDING = 2.
MIN_ACCEPTABLE_ESTIMATE = -5.0

# the size of time sequence chunks that are used to break the eatimation/data into pieces to speed up computation
# in units of time-scale parameter
# This is a tradeoff between looping through the data multiple times and having to do the fft inversion (n^2) of large time matrices
# If the bin size is 10 mins, and the and the time scale is 20 mins, then a value of 30 would give 30*20/10, which is a matrix size of 60.  Which is not that big.  
TIME_SEQUENCE_SIZE = 20.

# constants for outier, bad sensor removal
MAX_ALLOWED_PM2_5 = 1000.0
# constant to be used with MAD estimates
DEFAULT_OUTLIER_LEVEL = 5.0
# level below which outliers won't be removed 
MIN_OUTLIER_LEVEL = 10.0


def validateDate(dateString):
    """Check if date string is valid"""
    try:
        return dateString == datetime.strptime(dateString, DATETIME_FORMAT).strftime(DATETIME_FORMAT)
    except ValueError:
        return False


def parseDateString(datetime_string):
    """Parse date string into a datetime object"""
    return datetime.strptime(datetime_string, DATETIME_FORMAT).replace(tzinfo=timezone.utc)

#  this breaks the time part of the  eatimation/data into pieces to speed up computation
# sequence_size_mins
# assumes query_dates are sorted
def chunkTimeQueryData(query_dates, time_sequence_size, time_padding):
    # Careful.  Is the padding in date-time or just integer minutes.
    start_date = query_dates[0]
    end_date = query_dates[-1]
    query_length = (end_date - start_date)
#    query_length_mins = query_length.total_seconds()/60
    num_short_queries = int(query_length/time_sequence_size)
# cover the special corner case where the time series is shorter than the specified chunk size
    if (num_short_queries == 0):
        query_time_sequence = []
        query_time_sequence.append(query_dates)
    else:
        short_query_length = query_length/num_short_queries
        time_index = 0
        query_time_sequence = []
        for i in range(0,num_short_queries - 1):
            query_time_sequence.append([])
            while query_dates[time_index] < start_date + (i+1)*short_query_length:
                query_time_sequence[-1].append(query_dates[time_index])
                time_index += 1
# put the last sequence in place
        query_time_sequence.append([])
        for i in range(time_index,len(query_dates)):
            query_time_sequence[-1].append(query_dates[time_index])
            time_index += 1

# now build the endpoints we will need for the sensor data that feeds the estimates of each of these ranges of queries (they overlap)
    sensor_time_sequence = []
    for i in range(len(query_time_sequence)):
        sensor_time_sequence.append([query_time_sequence[i][0] - time_padding, query_time_sequence[i][-1] + time_padding])

    return sensor_time_sequence, query_time_sequence

# Load up elevation grid
# BE CAREFUL - this object, given the way the data is saved, seems to talk "lon-lat" order
def setupElevationInterpolator(filename):
    data = loadmat(filename)
    elevation_grid = data['elevs']
    gridLongs = data['gridLongs']
    gridLats = data['gridLats']
    # np.savetxt('grid_lats.txt',gridLats)
    # np.savetxt('grid_lons.txt',gridLongs)
    # np.savetxt('elev_grid.txt', elevation_grid)
    return interpolate.interp2d(gridLongs, gridLats, elevation_grid, kind='cubic', fill_value=0.0)


def loadBoundingBox(filename):
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        rows = [row for row in csv_reader][1:]
        bounding_box_vertices = [(index, float(row[1]), float(row[2])) for row, index in zip(rows, range(len(rows)))]
        return bounding_box_vertices


def loadCorrectionFactors(filename):
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        rows = [row for row in csv_reader]
        header = rows[0]
        rows = rows[1:]
        correction_factors = []
        for row in rows:
            rowDict = {name: elem for elem, name in zip(row, header)}
            rowDict['start_date'] = parseDateString(rowDict['start_date'])
            rowDict['end_date'] = parseDateString(rowDict['end_date'])
            rowDict['1003_slope'] = float(rowDict['1003_slope'])
            rowDict['1003_intercept'] = float(rowDict['1003_intercept'])
            rowDict['3003_slope'] = float(rowDict['3003_slope'])
            rowDict['3003_intercept'] = float(rowDict['3003_intercept'])
            rowDict['5003_slope'] = float(rowDict['5003_slope'])
            rowDict['5003_intercept'] = float(rowDict['5003_intercept'])
            correction_factors.append(rowDict)
        return correction_factors


def loadLengthScales(filename):
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        rows = [row for row in csv_reader]
        header = rows[0]
        rows = rows[1:]
        length_scales = []
        for row in rows:
            rowDict = {name: elem for elem, name in zip(row, header)}
            rowDict['start_date'] = parseDateString(rowDict['start_date'])
            rowDict['end_date'] = parseDateString(rowDict['end_date'])
            rowDict['latlon'] = float(rowDict['latlon'])
            rowDict['elevation'] = float(rowDict['elevation'])
            rowDict['time'] = float(rowDict['time'])
            length_scales.append(rowDict)
        return length_scales


def isQueryInBoundingBox(bounding_box_vertices, query_lat, query_lon):
    verts = [(0, 0)] * len(bounding_box_vertices)
    for elem in bounding_box_vertices:
        verts[elem[0]] = (elem[2], elem[1])
    # Add first vertex to end of verts so that the path closes properly
    verts.append(verts[0])
    codes = [Path.MOVETO]
    codes += [Path.LINETO] * (len(verts) - 2)
    codes += [Path.CLOSEPOLY]
    boundingBox = Path(verts, codes)
    return boundingBox.contains_point((query_lon, query_lat))


def removeInvalidSensors(sensor_data):
    # sensor is invalid if its average reading for any day exceeds 350 ug/m3
    epoch = datetime(1970, 1, 1)
    epoch = pytz.timezone('UTC').localize(epoch)
    dayCounts = {}
    dayReadings = {}
    for datum in sensor_data:
        pm25 = datum['PM2_5']
        datum['daysSinceEpoch'] = (datum['time'] - epoch).days
        key = (datum['daysSinceEpoch'], datum['ID'])
        if key in dayCounts:
            dayCounts[key] += 1
            dayReadings[key] += pm25
        else:
            dayCounts[key] = 1
            dayReadings[key] = pm25

    # get days that had higher than 350 avg reading
    keysToRemove = [key for key in dayCounts if (dayReadings[key] / dayCounts[key]) > 350]
    keysToRemoveSet = set()
    for key in keysToRemove:
        keysToRemoveSet.add(key)
        keysToRemoveSet.add((key[0] + 1, key[1]))
        keysToRemoveSet.add((key[0] - 1, key[1]))

    logging.info(f'Removing these days from data due to exceeding 350 ug/m3 avg: {keysToRemoveSet}')
    sensor_data = [datum for datum in sensor_data if (datum['daysSinceEpoch'], datum['ID']) not in keysToRemoveSet]

    # TODO NEEDS TESTING!
    # 5003 sensors are invalid if Raw 24-hour average PM2.5 levels are > 5 ug/m3
    # AND the two sensors differ by more than 16%
    sensor5003Locations = {
        datum['ID']: (datum['utm_x'], datum['utm_y']) for datum in sensor_data if datum['SensorModel'] == '5003'
    }
    sensorMatches = {}
    for sensor in sensor5003Locations:
        for match in sensor5003Locations:
            if sensor5003Locations[sensor] == sensor5003Locations[match] and sensor != match:
                sensorMatches[sensor] = match
                sensorMatches[match] = sensor

    keysToRemoveSet = set()
    for key in dayReadings:
        sensor = key[1]
        day = key[0]
        if sensor in sensorMatches:
            match = sensorMatches[sensor]
            reading1 = dayReadings[key] / dayCounts[key]
            key2 = (day, match)
            if key2 in dayReadings:
                reading2 = dayReadings[key2] / dayCounts[key2]
                difference = abs(reading1 - reading2)
                maximum = max(reading1, reading2)
                if min(reading1, reading2) > 5 and difference / maximum > 0.16:
                    keysToRemoveSet.add(key)
                    keysToRemoveSet.add((key[0] + 1, key[1]))
                    keysToRemoveSet.add((key[0] - 1, key[1]))
                    keysToRemoveSet.add(key2)
                    keysToRemoveSet.add((key2[0] + 1, key2[1]))
                    keysToRemoveSet.add((key2[0] - 1, key2[1]))

    logging.info((
        "Removing these days from data due to pair of 5003 sensors with both > 5 "
        f"daily reading and smaller is 16% different reading from larger : {keysToRemoveSet}"
    ))
    sensor_data = [datum for datum in sensor_data if (datum['daysSinceEpoch'], datum['ID']) not in keysToRemoveSet]

    # * Otherwise just average the two readings and correct as normal.
    return sensor_data


def applyCorrectionFactor(factors, data_timestamp, data, sensor_type):
    for factor in factors:
        factor_start = factor['start_date']
        factor_end = factor['end_date']
        if factor_start <= data_timestamp and factor_end > data_timestamp:
            if sensor_type == 'PMS1003':
                return data * factor['1003_slope'] + factor['1003_intercept']
            elif sensor_type == 'PMS3003':
                return data * factor['3003_slope'] + factor['3003_intercept']
            elif sensor_type == 'PMS5003':
                return data * factor['5003_slope'] + factor['5003_intercept']
###  print('\nNo correction factor found for ', data_timestamp)
#  no correction factor will be considered identity
# make sure corrected values are positive
    return np.maximum(data, 0.0)


def getScalesInTimeRange(scales, start_time, end_time):
    relevantScales = []
    for scale in scales:
        scale_start = scale['start_date']
        scale_end = scale['end_date']
        if start_time < scale_end and end_time >= scale_start:
            relevantScales.append(scale)
    return relevantScales


def interpolateQueryDates(start_datetime, end_datetime, period):
    query_dates = []
    query_date = start_datetime
    while query_date <= end_datetime:
        query_dates.append(query_date)
        query_date = query_date + timedelta(hours=period)

    return query_dates

# Not yet sure if this is needed
# build a grid of coordinates that will consistute the "map"
#def interpolateQueryLocationsUTM(lat_lo, lat_hi, lon_lo, lon_hi, spatial_res): 
    # # create the north sound and east west locations in UTM coordinates
    # E_range = np.arrange(lon_low, lon_hi, spatial_res)
    # N_range = np.arrange(lat_low, lat_hi, spatial_res)
    # return np.meshgrid(E_range, N_range)

# build a grid of coordinates that will consistute the "map"  - used for getEstimateMap() in the api
def interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_size, lon_size): 
#    lat_step = (lat_hi-lat_low)/float(lat_size)
#    lon_step = (lon_hi-lon_low)/float(lon_size)
    # lat_range = np.arange(lon_lo, lon_hi, lon_res)
    # lon_range = np.arange(lat_lo, lat_hi, lat_res)
    lat_range = np.linspace(lat_lo, lat_hi, lat_size, endpoint=True)
    lon_range = np.linspace(lon_lo, lon_hi, lon_size, endpoint=True)
    return lat_range, lon_range
#    return np.meshgrid(lat_range, lon_range)




# computes an approximate latlon bounding box that includes the given point and all points within the radius of distance_meters.  Used to limit the query of "relevant sensors".  Note the return order...
def latlonBoundingBox(lat, lon, distance_meters):
    E, N, zone_num, zone_let  = utm.from_latlon(lat, lon)
    lat_lo, lon_tmp = utm.to_latlon(E, N-distance_meters, zone_num, zone_let)
    lat_hi, lon_tmp = utm.to_latlon(E, N+distance_meters, zone_num, zone_let)
    lat_tmp, lon_lo = utm.to_latlon(E-distance_meters, N, zone_num, zone_let)
    lat_tmp, lon_hi = utm.to_latlon(E+distance_meters, N, zone_num, zone_let)
    return lat_lo, lat_hi, lon_lo, lon_hi

# when you have multiple queries at once, you need to build bounding boxes that include all of the sensors
def boundingBoxUnion(bbox1, bbox2):
    return min(bbox1[0], bbox2[0]), max(bbox1[1], bbox2[1]), min(bbox1[2], bbox2[2]), max(bbox1[3], bbox2[3])


# convenience/wrappers for the utm toolbox
def latlonToUTM(lat, lon):
    return utm.from_latlon(lat, lon)

def UTM(E, N, zone_num, zone_let):
    return utm.to_latlon(E, N)

def convertLatLonToUTM(sensor_data):
    for datum in sensor_data:
        datum['utm_x'], datum['utm_y'], datum['zone_num'], zone_let = latlonToUTM(datum['Latitude'], datum['Longitude'])

def getBigQueryClient():
    if not hasattr(getBigQueryClient, 'client'):
        getBigQueryClient.client = bigquery.Client()
    return getBigQueryClient.client

def estimateMedianDeviation(start_date, end_date, lat_lo, lat_hi, lon_lo, lon_hi, area_model):
    with open('common/db_table_headings.json') as json_file:
        db_table_headings = json.load(json_file)

    area_id_strings=area_model['idstring']
    query_list = []
#loop over all of the tables associated with this area model
    for area_id_string in area_id_strings:
        time_string = db_table_headings[area_id_string]['time']
        pm2_5_string = db_table_headings[area_id_string]['pm2_5']
        lon_string = db_table_headings[area_id_string]['longitude']
        lat_string = db_table_headings[area_id_string]['latitude']
        id_string = db_table_headings[area_id_string]['id']
        table_string = "telemetry.telemetry"

        column_string = " ".join([id_string, "AS id,", time_string, "AS time,", pm2_5_string, "AS pm2_5,", lat_string, "AS lat,", lon_string, "AS lon"])

        # if 'sensormodel' in db_table_headings[area_id_string]:
        #     sensormodel_string = db_table_headings[area_id_string]['sensormodel']
        #     column_string += ", " + sensormodel_string + " AS sensormodel"

        if 'sensormodel' in db_table_headings[area_id_string]:
            sensortype_string = db_table_headings[area_id_string]['sensormodel']
            column_string += ", " + sensortype_string + " AS sensormodel"

        where_string = ""
        if "label" in db_table_headings[area_id_string]:
            label_string = db_table_headings[area_id_string]["label"]
            column_string += ", " + label_string + " AS areamodel"
            where_string += " AND " + label_string + " = " + "'" + area_model["name"] + "'"

        query_list.append(f"""(SELECT pm2_5, id FROM (SELECT {column_string} FROM `{table_string}` WHERE (({time_string} > @start_date) AND ({time_string} < @end_date))) WHERE ((lat <= @lat_hi) AND (lat >= @lat_lo) AND (lon <= @lon_hi) AND (lon >= @lon_lo)) AND (pm2_5 < {MAX_ALLOWED_PM2_5}))""")

    query = "(" + " UNION ALL ".join(query_list) + ")"

#    query = f"SELECT PERCENTILE_DISC(pm2_5, 0.5) OVER ()  AS median FROM {query} LIMIT 1"
#    query = f"WITH all_data as {query} SELECT COUNT (DISTINCT id) as num_sensors, PERCENTILE_DISC(pm2_5, 0.0) OVER ()  AS min,  PERCENTILE_DISC(pm2_5, 0.5) OVER ()  AS median, PERCENTILE_DISC(pm2_5, 1.0) OVER ()  AS max FROM all_data LIMIT 1"
    full_query = f"WITH all_data as {query} SELECT * FROM (SELECT PERCENTILE_DISC(pm2_5, 0.5) OVER() AS median FROM all_data LIMIT 1) JOIN (SELECT COUNT(DISTINCT id) as num_sensors FROM all_data) ON TRUE"

#        query_string = f"""SELECT pm2_5 FROM (SELECT {column_string} FROM `{db_id_string}` WHERE (({time_string} > {start_date}) AND ({time_string} < {end_date}))) WHERE ((lat <= {lat_hi}) AND (lat >= {lat_lo}) AND (lon <= {lon_hi}) AND (lon >= {lon_lo})) ORDER BY time ASC"""

#    print("query is: " + full_query)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            # bigquery.ScalarQueryParameter("lat", "NUMERIC", lat),
            # bigquery.ScalarQueryParameter("lon", "NUMERIC", lon),
            # bigquery.ScalarQueryParameter("radius", "NUMERIC", radius),
            bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
            bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            bigquery.ScalarQueryParameter("lat_lo", "NUMERIC", lat_lo),
            bigquery.ScalarQueryParameter("lat_hi", "NUMERIC", lat_hi),
            bigquery.ScalarQueryParameter("lon_lo", "NUMERIC", lon_lo),
            bigquery.ScalarQueryParameter("lon_hi", "NUMERIC", lon_hi),
        ]
    )

    bq_client = getBigQueryClient()
    query_job = bq_client.query(full_query, job_config=job_config)
    if query_job.error_result:
        return "Invalid API call - check documentation.", 400

    median_data = query_job.result()
    for row in median_data:
        median = row.median
        count = row.num_sensors

    full_query = f"WITH all_data as {query} SELECT PERCENTILE_DISC(ABS(pm2_5 - {median}), 0.5) OVER() AS median FROM all_data LIMIT 1"
    query_job = bq_client.query(full_query, job_config=job_config)
    if query_job.error_result:
        return "Invalid API call - check documentation.", 400
    MAD_data = query_job.result()
    for row in MAD_data:
        MAD = row.median

    return median, MAD, count

def filterUpperLowerBounds(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model, filter_level = DEFAULT_OUTLIER_LEVEL):
        median, MAD, count = estimateMedianDeviation(start_date, end_date, lat_lo, lat_hi, lon_lo, lon_hi, area_model)
        lo = max(median - filter_level*MAD, 0.0)
        hi = min(max(median + filter_level*MAD, MIN_OUTLIER_LEVEL), MAX_ALLOWED_PM2_5)
        return lo, hi

def filterUpperLowerBoundsForArea(start_date, end_date, area_model, filter_level = DEFAULT_OUTLIER_LEVEL):
        bbox_array = np.array(area_model['boundingbox'])[:,1:3]
        lo = bbox_array.min(axis=0)
        hi = bbox_array.max(axis=0)
        median, MAD, count = estimateMedianDeviation(start_date, end_date, lo[0], hi[0], lo[1], hi[1], area_model)
        lo = max(median - filter_level*MAD, 0.0)
        hi = min(max(median + filter_level*MAD, MIN_OUTLIER_LEVEL), MAX_ALLOWED_PM2_5)
        print(f"Hi and low bounds are {hi} and {lo}")
        return lo, hi

def submit_sensor_query(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model, min_value, max_value):
#    print("aread_id_string: " + area_id_string)
#    db_id_string = "tetrad-296715.telemetry.slc_ut"
    with open('common/db_table_headings.json') as json_file:
        db_table_headings = json.load(json_file)

    area_id_strings = area_model["idstring"]
    this_area_model = area_model["name"]
        
    query_list = []
#loop over all of the tables associated with this area model
    for area_id_string in area_id_strings:
        time_string = db_table_headings[area_id_string]['time']
        pm2_5_string = db_table_headings[area_id_string]['pm2_5']
        lon_string = db_table_headings[area_id_string]['longitude']
        lat_string = db_table_headings[area_id_string]['latitude']
        id_string = db_table_headings[area_id_string]['id']
        table_string = "telemetry.telemetry"

        column_string = " ".join([id_string, "AS id,", time_string, "AS time,", pm2_5_string, "AS pm2_5,", lat_string, "AS lat,", lon_string, "AS lon"])

        if 'sensormodel' in db_table_headings[area_id_string]:
            sensormodel_string = db_table_headings[area_id_string]['sensormodel']
            column_string += ", " + sensormodel_string + " AS sensormodel"

        if 'sensorsource' in db_table_headings[area_id_string]:
            sensorsource_string = db_table_headings[area_id_string]['sensorsource']
            column_string += ", " + sensorsource_string + " AS sensorsource"

        where_string = ""
        if "label" in db_table_headings[area_id_string]:
            label_string = db_table_headings[area_id_string]["label"]
            column_string += ", " + label_string + " AS areamodel" 
            where_string += " AND " + label_string + " = " + "'" + this_area_model + "'"

        query_list.append(f"""(SELECT * FROM (SELECT {column_string} FROM `{table_string}` WHERE (({time_string} > '{start_date}') AND ({time_string} < '{end_date}')) {where_string}) WHERE ((lat <= {lat_hi}) AND (lat >= {lat_lo}) AND (lon <= {lon_hi}) AND (lon >= {lon_lo})) AND (pm2_5 < {max_value}) AND (pm2_5 > {min_value}))""")


    query = " UNION ALL ".join(query_list) + " ORDER BY time ASC "
    print(f"submit sensor query is {query}")

#        query_string = f"""SELECT * FROM (SELECT {column_string} FROM `{db_id_string}` WHERE (({time_string} > {start_date}) AND ({time_string} < {end_date}))) WHERE ((lat <= {lat_hi}) AND (lat >= {lat_lo}) AND (lon <= {lon_hi}) AND (lon >= {lon_lo})) ORDER BY time ASC"""

    
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
            bigquery.ScalarQueryParameter("lat_lo", "NUMERIC", lat_lo),
            bigquery.ScalarQueryParameter("lat_hi", "NUMERIC", lat_hi),
            bigquery.ScalarQueryParameter("lon_lo", "NUMERIC", lon_lo),
            bigquery.ScalarQueryParameter("lon_hi", "NUMERIC", lon_hi),
        ]
    )

    bq_client = getBigQueryClient()
    query_job = bq_client.query(query, job_config=job_config)

    if query_job.error_result:
        return "Invalid API call - check documentation.", 400
    # Waits for query to finish
    sensor_data = query_job.result()

    return(sensor_data)


# could do an ellipse in lat/lon around the point using something like this
#WHERE SQRT(POW(Latitude - @lat, 2) + POW(Longitude - @lon, 2)) <= @radius
#    AND time > @start_date AND time < @end_date
#    ORDER BY time ASC
# Also could do this by spherical coordinates on the earth -- however we use a lat-lon box to save compute time on the BigQuery server

# radius should be in *meters*!!!
# this has been modified so that it now takes an array of lats/lons
# the radius parameter is not implemented in a precise manner -- rather it is converted to a lat-lon bounding box and all within that box are returned
# there could be an additional culling of sensors outside the radius done here after the query - if the radius parameter needs to be precise. 
def request_model_data_local(lats, lons, radius, start_date, end_date, area_model, outlier_filtering = True):
    model_data = []
    # get the latest sensor data from each sensor
    # Modified by Ross for
    ## using a bounding box in lat-lon
    if isinstance(lats, (float)):
            if isinstance(lons, (float)):
                    lat_lo, lat_hi, lon_lo, lon_hi = common.utils.latlonBoundingBox(lats, lons, radius)
            else:
                    return "lats,lons data structure misalignment in request sensor data", 400
    elif (isinstance(lats, (np.ndarray)) and isinstance(lons, (np.ndarray))):
        if not lats.shape == lons.shape:
            return "lats,lons data data size error", 400
        else:
            num_points = lats.shape[0]
            lat_lo, lat_hi, lon_lo, lon_hi = common.utils.latlonBoundingBox(lats[0], lons[0], radius)
            for i in range(1, num_points):
                lat_lo, lat_hi, lon_lo, lon_hi = common.utils.boundingBoxUnion((common.utils.latlonBoundingBox(lats[i], lons[i], radius)), (lat_lo, lat_hi, lon_lo, lon_hi))
    else:
        return "lats,lons data structure misalignment in request sensor data", 400

    if outlier_filtering:
        min_value, max_value = filterUpperLowerBounds(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model)
    else:
        min_value = 0.0
        max_value = MAX_ALLOWED_PM2_5
    rows = submit_sensor_query(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model, min_value, max_value)

#    print(rows)
    for row in rows:
        new_row = {
            "ID": row.id,
            "Latitude": row.lat,
            "Longitude": row.lon,
            "time": row.time,
            "PM2_5": row.pm2_5,
            "SensorModel":row.sensormodel,
            "SensorSource":row.sensorsource
            }

        #this is taken care of in the query now
        # if 'sensormodel' in row:
        #     new_row["SensorModel"] = row.sensormodel
        # else:
        #     print(f"missed sensor model for row {row}")
        #     new_row["SensorModel"] = "default"
        # try:
        #     new_row["SensorModel"] = row.sensormodel
        # except:
        #     new_row["SensorModel"] = "Default"

        # try:
        #     new_row["SensorSource"] = row.sensorsource
        # except:
        #     new_row["SensorSource"] = "Default"

        model_data.append(new_row)

    return model_data

def computeEstimatesForLocations(query_dates, query_locations, area_model, outlier_filtering = True):
    num_locations = query_locations.shape[0]
    query_lats = query_locations[:,0]
    query_lons = query_locations[:,1]
    query_start_datetime = query_dates[0]
    query_end_datetime = query_dates[-1]

    elevation_interpolator = common.jsonutils.buildAreaElevationInterpolator(area_model['elevationfile'])    
    query_elevations = np.array([elevation_interpolator(this_row[1], this_row[0])[0] for this_row in query_locations])
    
    # step 0, load up the bounding box from file and check that request is within it

    # for i in range(num_locations):
    #     if not jsonutils.isQueryInBoundingBox(area_model['boundingbox'], query_lats[i], query_lons[i]):
    #         app.logger.error(f"The query location, {query_lats[i]},{query_lons[i]},  is outside of the bounding box.")
    #         return np.full((query_lats.shape[0], len(query_dates)), 0.0), np.full((query_lats.shape[0], len(query_dates)), np.nan), ["Query location error" for i in query_dates]

    # step 2, load up length scales from file

    latlon_length_scale, time_length_scale, elevation_length_scale = common.jsonutils.getLengthScalesForTime(area_model['lengthscales'], query_start_datetime)
    if latlon_length_scale == None:
            return np.full((query_lats.shape[0], query_dates.shape[0]), 0.0), np.full((query_lats.shape[0], query_dates.shape[0]), np.nan), ["Length scale parameter error" for i in range(query_dates.shape[0])]

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
            radius,
            query_start_datetime - timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale),
            query_end_datetime + timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale),
            area_model, outlier_filtering)

    unique_sensors = {datum['ID'] for datum in sensor_data}

    # step 3.5, convert lat/lon to UTM coordinates
    try:
        common.utils.convertLatLonToUTM(sensor_data)
    except ValueError as err:
        return np.full((query_lats.shape[0], query_dates.shape[0]), 0.0), np.full((query_lats.shape[0], query_dates.shape[0]), np.nan), ["Failure to convert lat/lon" for i in range(query_dates.shape[0])]

# legacy code forcing sensors to like in UTM zone...
#    sensor_data = [datum for datum in sensor_data if datum['zone_num'] == 12]

    unique_sensors = {datum['ID'] for datum in sensor_data}

    # Step 4, parse sensor type from the version
#    sensor_source_to_type = {'AirU': '3003', 'PurpleAir': '5003', 'DAQ': '0000', 'Default':'Default'}
# DAQ does not need a correction factor
#    for datum in sensor_data:
#        datum['type'] =  sensor_source_to_type[datum['SensorSource']]

    if len(sensor_data) > 0:
        print(f'Fields: {sensor_data[0].keys()}')
    else:
        return np.full((query_lats.shape[0], query_dates.shape[0]), 0.0), np.full((query_lats.shape[0], query_dates.shape[0]), np.nan), ["Zero sensor data" for i in range(query_dates.shape[0])]

    # step 4.5, Data Screening
#    print('Screening data')
    sensor_data = common.utils.removeInvalidSensors(sensor_data)

    # step 5, apply correction factors to the data
    for datum in sensor_data:
        datum['PM2_5'] = common.jsonutils.applyCorrectionFactor(area_model['correctionfactors'], datum['time'], datum['PM2_5'], datum['SensorModel'])

    # step 6, add elevation values to the data
    # NOTICE - the elevation object takes locations in the form "lon-lat"
    # this seems redundant since elevations are passed in...
    # elevation_interpolator = jsonutils.buildAreaElevationInterpolator(area_model['elevationfile'])
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
    sensor_sequence, query_sequence = common.utils.chunkTimeQueryData(query_dates, time_sequence_length, time_padding)

    yPred = np.empty((num_locations, 0))
    yVar = np.empty((num_locations, 0))
    status = []
    if len(sensor_data) == 0:
        status = "0 sensors/measurements"
        return 
    for i in range(len(query_sequence)):
    # step 7, Create Model
        model, time_offset, model_status = common.gaussian_model_utils.createModel(
            sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale, sensor_sequence[i][0], sensor_sequence[i][1], save_matrices=True)
        # check to see if there is a valid model
        if (model == None):
            yPred_tmp = np.full((query_lats.shape[0], len(query_sequence[i])), 0.0)
            yVar_tmp = np.full((query_lats.shape[0], len(query_sequence[i])), np.nan)
            status_estimate_tmp = [model_status for i in range(len(query_sequence[i]))]
        else:
            yPred_tmp, yVar_tmp, status_estimate_tmp = common.gaussian_model_utils.estimateUsingModel(
                model, query_lats, query_lons, query_elevations, query_sequence[i], time_offset, save_matrices=True)
        # put the estimates together into one matrix
        yPred = np.concatenate((yPred, yPred_tmp), axis=1)
        yVar = np.concatenate((yVar, yVar_tmp), axis=1)
        status = status + status_estimate_tmp

    if np.min(yPred) < MIN_ACCEPTABLE_ESTIMATE:
        print("got estimate below level " + str(MIN_ACCEPTABLE_ESTIMATE))
        
    # Here we clamp values to ensure that small negative values to do not appear
    yPred = np.clip(yPred, a_min = 0., a_max = None)

    return yPred, yVar, query_elevations, status