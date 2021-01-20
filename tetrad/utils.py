from os import getenv
from datetime import datetime, timedelta
from pytz import timezone
from utm import from_latlon
from matplotlib.path import Path
from scipy import interpolate
from scipy.io import loadmat
from dotenv import load_dotenv
from csv import reader as csv_reader
import math 
from flask import jsonify
import numpy as np

#load_dotenv()
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
BQ_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S America/Denver"


def validateDate(dateString):
    """Check if date string is valid"""
    try:
        return dateString == datetime.strptime(dateString, DATETIME_FORMAT).strftime(DATETIME_FORMAT)
    except ValueError:
        return False


def validLatitude(lat):
    """Check if latitude is valid"""
    return (-90 <= lat <= 90)


def validLongitude(lon):
    """Check if longitude is valid"""
    return (-180 <= lon <= 180)


def validLatLon(lat, lon):
    """Check if lat/lon are valid"""
    return validLatitude(lat) and validLongitude(lon)


def validRadius(radius):
    """Check if valid radius for Earth"""
    return (0 < radius < 6.3e6)


def parseDateString(datetime_string):
    """Parse date string into a datetime object"""
    return datetime.strptime(datetime_string, DATETIME_FORMAT).astimezone(timezone('US/Mountain'))


def datetimeToBigQueryTimestamp(date):
    return date.strftime(BQ_DATETIME_FORMAT)


# # Load up elevation grid
def setupElevationInterpolator():
    data = loadmat(getenv("ELEVATION_MAP_FILENAME"))
    elevation_grid = data['elevs']
    gridLongs = data['gridLongs']
    gridLats = data['gridLats']
    return interpolate.interp2d(gridLongs, gridLats, elevation_grid, kind='cubic')


def loadBoundingBox():
    with open(getenv("BOUNDING_BOX_FILENAME")) as csv_file:
        read_csv = csv_reader(csv_file, delimiter=',')
        rows = [row for row in read_csv][1:]
        bounding_box_vertices = [(index, float(row[1]), float(row[2])) for row, index in zip(rows, range(len(rows)))]
        return bounding_box_vertices


def loadCorrectionFactors():
    with open(getenv("CORRECTION_FACTORS_FILENAME")) as csv_file:
        read_csv = csv_reader(csv_file, delimiter=',')
        rows = [row for row in read_csv]
        header = rows[0]
        rows = rows[1:]
        correction_factors = []
        for row in rows:
            rowDict = {name: elem for elem, name in zip(row, header)}
            rowDict['start_date'] = parseDateString(rowDict['start_date'])
            rowDict['end_date'] = parseDateString(rowDict['end_date'])
            rowDict['3003_slope'] = float(rowDict['3003_slope'])
            rowDict['3003_intercept'] = float(rowDict['3003_intercept'])
            correction_factors.append(rowDict)
        return correction_factors


def applyCorrectionFactor(factors, data_timestamp, data):
    for factor in factors:
        factor_start = factor['start_date']
        factor_end = factor['end_date']
        if factor_start <= data_timestamp and factor_end > data_timestamp:
            return data * factor['3003_slope'] + factor['3003_intercept']
    print('\nNo correction factor found for ', data_timestamp)
    return data


def applyCorrectionFactorsToList(data_list, pm_key=None):
    """Apply correction factors (in place) to PM2.5 data in data_list"""
    
    # Open the file and get correction factors
    with open(getenv("CORRECTION_FACTORS_FILENAME")) as csv_file:
        read_csv = csv_reader(csv_file, delimiter=',')
        rows = [row for row in read_csv]
        header = rows[0]
        rows = rows[1:]
        correction_factors = []
        for row in rows:
            rowDict = {name: elem for elem, name in zip(row, header)}
            rowDict['start_date'] = parseDateString(rowDict['start_date'])
            rowDict['end_date'] = parseDateString(rowDict['end_date'])
            rowDict['3003_slope'] = float(rowDict['3003_slope'])
            rowDict['3003_intercept'] = float(rowDict['3003_intercept'])
            correction_factors.append(rowDict)
        
    # Apply the correction factors to the PM2.5 data
    for datum in data_list:
        try:
            datum[pm_key] = applyCorrectionFactor(correction_factors, datum['Timestamp'], datum[pm_key])
        except: # Only try once. We just assume it isn't there if the first row doesn't have it
            return data_list
        # found = False
        # for factor in correction_factors:
        #     factor_start = factor['start_date']
        #     factor_end = factor['end_date']
        #     if factor_start <= datum['Timestamp'] < factor_end:
        #         datum['PM2_5'] = datum['PM2_5'] * factor['3003_slope'] + factor['3003_intercept']
        #         found = True
        #         break
        # if not found:
        #     print('\nNo correction factor found for ', datum['Timestamp'])
    return data_list


def tuneData(data:list, pm_key=None, temp_key=None, hum_key=None):
    """ Clean data and apply correction factors """
    # Open the file and get correction factors
    with open(getenv("CORRECTION_FACTORS_FILENAME")) as csv_file:
        read_csv = csv_reader(csv_file, delimiter=',')
        rows = [row for row in read_csv]
        header = rows[0]
        rows = rows[1:]
        correction_factors = []
        for row in rows:
            rowDict = {name: elem for elem, name in zip(row, header)}
            rowDict['start_date'] = parseDateString(rowDict['start_date'])
            rowDict['end_date'] = parseDateString(rowDict['end_date'])
            rowDict['3003_slope'] = float(rowDict['3003_slope'])
            rowDict['3003_intercept'] = float(rowDict['3003_intercept'])
            correction_factors.append(rowDict)
        
    goodPM, goodTemp, goodHum = True, True, True
    for datum in data:
        if pm_key and goodPM:
            try:
                if (datum[pm_key] == getenv("PM_BAD_FLAG")) or (datum[pm_key] >= getenv("PM_BAD_THRESH")):
                    datum[pm_key] = None
                else:
                    datum[pm_key] = applyCorrectionFactor(correction_factors, datum['Timestamp'], datum[pm_key])
            except:
                goodPM = False

        if temp_key and goodTemp:
            try:
                if datum[temp_key] == getenv("TEMP_BAD_FLAG"):
                    datum[temp_key] = None 
            except:
                goodTemp = False

        if hum_key and goodHum:
            try:
                if datum[hum_key] == getenv("HUM_BAD_FLAG"):
                    datum[hum_key] = None 
            except:
                goodHum = False
        
    return data


        

def loadLengthScales():
    with open(getenv("LENGTH_SCALES_FILENAME")) as csv_file:
        read_csv = csv_reader(csv_file, delimiter=',')
        rows = [row for row in read_csv]
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
    epoch = timezone('US/Mountain').localize(epoch)
    dayCounts = {}
    dayReadings = {}

    # Accumulate total PM and # entries for each sensor and each day
    for datum in sensor_data:
        pm25 = datum['PM2_5']
        datum['daysSinceEpoch'] = (datum['Timestamp'] - epoch).days
        key = (datum['daysSinceEpoch'], datum['DeviceID'])
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

    print(f'Removing these days from data due to exceeding 350 ug/m3 avg: {keysToRemoveSet}')
    sensor_data = [datum for datum in sensor_data if (datum['daysSinceEpoch'], datum['DeviceID']) not in keysToRemoveSet]

    # TODO NEEDS TESTING!
    # 5003 sensors are invalid if Raw 24-hour average PM2.5 levels are > 5 ug/m3
    # AND the two sensors differ by more than 16%
    # sensor5003Locations = {
    #     datum['ID']: (datum['utm_x'], datum['utm_y']) for datum in sensor_data if datum['type'] == '5003'
    # }
    # sensorMatches = {}
    # for sensor in sensor5003Locations:
    #     for match in sensor5003Locations:
    #         if sensor5003Locations[sensor] == sensor5003Locations[match] and sensor != match:
    #             sensorMatches[sensor] = match
    #             sensorMatches[match] = sensor
    #
    # keysToRemoveSet = set()
    # for key in dayReadings:
    #     sensor = key[1]
    #     day = key[0]
    #     if sensor in sensorMatches:
    #         match = sensorMatches[sensor]
    #         reading1 = dayReadings[key] / dayCounts[key]
    #         key2 = (day, match)
    #         if key2 in dayReadings:
    #             reading2 = dayReadings[key2] / dayCounts[key2]
    #             difference = abs(reading1 - reading2)
    #             maximum = max(reading1, reading2)
    #             if min(reading1, reading2) > 5 and difference / maximum > 0.16:
    #                 keysToRemoveSet.add(key)
    #                 keysToRemoveSet.add((key[0] + 1, key[1]))
    #                 keysToRemoveSet.add((key[0] - 1, key[1]))
    #                 keysToRemoveSet.add(key2)
    #                 keysToRemoveSet.add((key2[0] + 1, key2[1]))
    #                 keysToRemoveSet.add((key2[0] - 1, key2[1]))
    #
    # print((
    #     "Removing these days from data due to pair of 5003 sensors with both > 5 "
    #     f"daily reading and smaller is 16% different reading from larger : {keysToRemoveSet}"
    # ))
    # sensor_data = [datum for datum in sensor_data if (datum['daysSinceEpoch'], datum['ID']) not in keysToRemoveSet]

    # * Otherwise just average the two readings and correct as normal.
    return sensor_data


def getScalesInTimeRange(scales, start_time, end_time):
    relevantScales = []
    if start_time == end_time:
        start_time = start_time - timedelta(days=1)
        end_time = end_time + timedelta(days=1)
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

def interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_size, lon_size):
    lat_vector = np.linspace(lat_lo, lat_hi, lat_size)
    lon_vector = np.linspace(lon_lo, lon_hi, lon_size)

    return lon_vector, lat_vector


def latlonToUTM(lat, lon):
    return from_latlon(lat, lon)


# TODO: Rename
def convertLatLonToUTM(sensor_data):
    for datum in sensor_data:
        datum['utm_x'], datum['utm_y'], datum['zone_num'], zone_let = latlonToUTM(datum['Latitude'], datum['Longitude'])


def idsToWHEREClause(ids, id_tbl_name):
    """
    Return string that looks like:
    (<id_tbl_name> = <id[0]> OR ... OR <id_tbl_name> = <id[n-1]>)
    """
    if isinstance(ids, str):
        ids = [ids]
    if not isinstance(ids, list):
        raise TypeError('ids must be single DeviceID or DeviceID list')

    return """({})""".format(' OR '.join([f'{id_tbl_name} = "{ID}"' for ID in ids]))


def checkArgs(request_args, required_args):
    if not all(elem in list(request_args) for elem in list(required_args)):
        return jsonify({'Error': f'Missing arg, one of: {", ".join(list(required_args))}'}), 400
    else:
        return True, 200

