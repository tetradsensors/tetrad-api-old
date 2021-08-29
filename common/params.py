from enum import Enum
from datetime import datetime, time
import common.utils
import common.jsonutils
import numpy as np

class URL_PARAMS(str, Enum):
    START_TIME = "startTime"
    END_TIME = "endTime"
    ID = "id"
    NO_CORRECTION = "noCorrection"
    AREA_MODEL = "areaModel"
    SENSOR_SOURCE = "sensorSource"
    FUNCTION = "function"
    GROUP_BY = "groupBy"
    TIME_INTERVAL = "timeInterval"
    APPLY_CORRECTION = "applyCorrection"
    FLAG_OUTLIERS = "flagOutliers"
    LAT = "lat"
    LON = "lon"
    RADIUS = "radius"
    TIME = "time"
    LAT_LO = "latLo"
    LAT_HI = "latHi"
    LON_LO = "lonLo"
    LON_HI = "lonHi"
    LAT_SIZE = "latSize"
    LON_SIZE = "lonSize"
    LATS = "lats"
    LONS = "lons"
    KEY = 'key'
    USERNAME = 'username'
    PASSCODE = 'passcode'
    SERVICE_NAME = 'serviceName'
    HOURS_TO_RECHARGE = 'hoursToRecharge'
    LIMIT = 'limit'
    REFERRER = 'referrer'
    DEFAULT_LIMIT = 'defaultLimit'
    DEFAULT_HOURS_TO_RECHARGE = 'defaultHoursToRecharge'

    def __str__(self):
        return "'" + self.value + "'"


class PARAMS_HELP_MESSAGES(str, Enum):
    START_TIME = f"{URL_PARAMS.START_TIME} must be in ISO 8601 format (2020-01-01T00:00:00+00) and specifies the start of the query."
    END_TIME = f"{URL_PARAMS.END_TIME} must be in ISO 8601 format (2020-01-01T00:00:00+00) and specifies the end time of the query."
    ID = "Single or multiple (comma separated) sensor IDs."
    NO_CORRECTION = "noCorrection"
    AREA_MODEL = "Specifies a particular area model to use for this query.  The default is to find an area model that contains the upper-right corner of the box."
    AREA_MODEL_AS_LIST = f"Single or multiple (comma separated), specifies a particular area model to use for this query. Area parameters include: {', '.join(list(common.jsonutils.getAreaModels().keys()))}"
    SENSOR_SOURCE = "Single or multiple (comma separated) sensor source."
    FLAG_OUTLIERS = "If this parameter is given, then each measurement has an associated “status” entry in the record that indicates whether the measurement has been identified as an outlier using the mean absolute deviation method (MAD)."
    FUNCTION = "one of mean, min, or max"
    GROUP_BY = "one of id, sensorModel, or area.  Default is to apply the function over all sensors at the specified times/intervals"
    TIME_INTERVAL = "The time between individual maps, given in minutes"
    TIME_INTERVAL_HR = "The time between individual maps, given in hours (or parts of hours, e.g. 0.25 is 15 minutes)."
    APPLY_CORRECTION = f"{URL_PARAMS.APPLY_CORRECTION}"
    LAT = "must be between -90, 90"
    LON = "must be between -180, 180"
    RADIUS = "radius"
    TIME = "The datetime to take a single-time estimate map (grid).  Required if start/end are not specified.  This parameter also accepts “time=now” to get an estimate at the current time."
    LAT_LO = "Lower bounds of the latitude box that defines the extent of the map."
    LAT_HI = "Upper bounds of the latitude box that defines the extent of the map."
    LON_LO = "Lower bounds of the longitude box that defines the extent of the map."
    LON_HI = "Upper bounds of the longitude box that defines the extent of the map."
    LAT_SIZE = "Integers that define the number of grid points that will be estimated along the latitude directions for the map."
    LON_SIZE = "Integers that define the number of grid points that will be estimated along the longitude directions for the map."
    LATS = "Single value or list of lats. lats must be between -90, 90"
    LONS = "Single value or list of lons. lons must be between -180, 180"


def time_interval_param(value):
    if float(value) <= 0:
        raise ValueError(PARAMS_HELP_MESSAGES.TIME_INTERVAL)
    else:
        return float(value)

def lat_size_param(value):
    if float(value) <= 0:
        raise ValueError(PARAMS_HELP_MESSAGES.LAT_SIZE)
    else:
        return float(value)

def lon_size_param(value):
    if float(value) <= 0:
        raise ValueError(PARAMS_HELP_MESSAGES.LON_SIZE)
    else:
        return float(value)

def list_param(value):
    if len(value) > 0:
        return value.split(',')
    else:
        return []

def multi_area(value):
    allAreas = list(common.jsonutils.getAreaModels().keys())
    value = value.split(',')
    if value == ['all']:
        return allAreas
    if all(v in allAreas for v in value):
        return value
    else:
        raise ValueError(PARAMS_HELP_MESSAGES.AREA_MODEL_AS_LIST)

def function_parse(value):
    if value in ['mean', 'max', 'min']:
        return value
    else:
        raise ValueError(PARAMS_HELP_MESSAGES.FUNCTION)

def groupby_parse(value):
    if value in ['id', 'sensorModel', 'area']:
        return value
    else:
        raise ValueError(PARAMS_HELP_MESSAGES.GROUP_BY)

def bool_flag(value):
    """
    Handle the ability to pass a parameter that can
    have no value or a boolean value. No value gives
    True (like the switch is on). Absense of the value
    returns False (switch is off). You can also pass
    "TRUE", "True", "true", "1", "FALSE", "False", 
    "false", "0" like: param=True
    """
    if value is None:
        return False
    if value == '':
        return True
    if value.lower() == 'true':
        return True
    if value == "1":
        return True
    if value.lower() == 'false':
        return False
    if value == "0":
        return False

def str_lower(value):
    return value.lower()

def lat_check(value):
    try:
        value = float(value)
        assert (-90 < value < 90)
        return value
    except:
        raise ValueError(PARAMS_HELP_MESSAGES.LAT)

def lon_check(value):
    try:
        value = float(value)
        assert (-180 <= value <= 180)
        return value
    except:
        raise ValueError(PARAMS_HELP_MESSAGES.LON)

def lats_parse(value):
    value = value.split(',')
    return np.array([lat_check(v) for v in value])

def lons_parse(value):
    value = value.split(',')
    return np.array([lon_check(v) for v in value])
    
def radius_check_m(value):
    try:
        value = float(value)
        assert (0 < value <= 6.371e6)
        return value
    except:
        raise ValueError(PARAMS_HELP_MESSAGES.RADIUS)

def radius_check_km(value):
    try:
        value = float(value)
        assert (0 < value <= 6.371e3)
        return value
    except:
        raise ValueError(PARAMS_HELP_MESSAGES.RADIUS)