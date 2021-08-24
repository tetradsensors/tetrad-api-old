from enum import Enum
from datetime import datetime, time
import common.utils
import common.jsonutils

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
    PASSCODE = 'passcode'
    SERVICE_NAME = 'service_name'
    HOURS_TO_RECHARGE = 'hours_to_recharge'
    LIMIT = 'limit'


class PARAMS_HELP_MESSAGES(str, Enum):
    START_TIME = f"{URL_PARAMS.START_TIME} must be in ISO 8601 format (2020-01-01T00:00:00+00) and specifies the start of the query."
    END_TIME = f"{URL_PARAMS.END_TIME} must be in ISO 8601 format (2020-01-01T00:00:00+00) and specifies the end time of the query."
    ID = "Single or multiple (comma separated) sensor IDs."
    NO_CORRECTION = "noCorrection"
    AREA_MODEL = "Specifies a particular area model to use for this query.  The default is to find an area model that contains the upper-right corner of the box."
    AREA_MODEL_AS_LIST = "Single or multiple (comma separated), specifies a particular area model to use for this query."
    SENSOR_SOURCE = "Single or multiple (comma separated) sensor source."
    FLAG_OUTLIERS = "If this parameter is given, then each measurement has an associated “status” entry in the record that indicates whether the measurement has been identified as an outlier using the mean absolute deviation method (MAD)."
    FUNCTION = "one of mean, min, or max"
    GROUP_BY = "one of id, sensorSource, or area.  Default is to apply the function over all sensors at the specified times/intervals"
    TIME_INTERVAL = "The time between individual maps, given in hours (or parts of hours, e.g. 0.25 is 15 minutes)."
    APPLY_CORRECTION = f"{URL_PARAMS.APPLY_CORRECTION}"
    LAT = "lat"
    LON = "lon"
    RADIUS = "radius"
    TIME = "The datetime to take a single-time estimate map (grid).  Required if start/end are not specified.  This parameter also accepts “time=now” to get an estimate at the current time."
    LAT_LO = "Lower bounds of the latitude box that defines the extent of the map."
    LAT_HI = "Upper bounds of the latitude box that defines the extent of the map."
    LON_LO = "Lower bounds of the longitude box that defines the extent of the map."
    LON_HI = "Upper bounds of the longitude box that defines the extent of the map."
    LAT_SIZE = "Integers that define the number of grid points that will be estimated along the latitude directions for the map."
    LON_SIZE = "Integers that define the number of grid points that will be estimated along the longitude directions for the map."
    LATS = "lats"
    LONS = "lons"


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
    if value in ['id', 'sensorSource', 'area']:
        return value
    else:
        raise ValueError(PARAMS_HELP_MESSAGES.GROUP_BY)
