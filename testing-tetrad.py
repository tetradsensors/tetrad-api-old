import urllib.request
import json
import itertools
import csv
import pandas as pd
import ssl
import numpy as np
import datetime 
myssl = ssl.create_default_context();
myssl.check_hostname=False
myssl.verify_mode=ssl.CERT_NONE

class bc:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    def o(s):
        return bc.WARNING + s + bc.ENDC
    def r(s):
        return bc.FAIL + s + bc.ENDC
    def g(s):
        return bc.OKGREEN + s + bc.ENDC 
    def b(s):
        return bc.OKBLUE + s + bc.ENDC 
    def c(s):
        return bc.OKCYAN + s + bc.ENDC

sources = ['Tetrad', 'AQ%26U', 'PurpleAir', 'all']
regions = ['slc_ut', 'chatt_tn', 'clev_oh', 'kc_mo', 'pv_ma', 'all']

remote_server = "https://tetrad-api-qnofmwqtgq-uc.a.run.app/api"
local_server = "https://127.0.0.1:8080/api"

server = remote_server

def randomRegion():
    return regions[np.random.randint(low=0, high=len(regions))]

def randomSource():
    return sources[np.random.randint(low=0, high=len(sources))]

def randomRegionAndSource():
    r, s = randomRegion(), randomSource()
    while (r == 'AQ%26U' and s != 'slc_ut'):
        r, s = randomRegion(), randomSource()
    return r, s

def randomDateRange(delta_min=3, delta_max=72, units='hours', start=None, end=None):
    delta = np.random.randint(low=delta_min, high=delta_max)
    return randomDatesInRange(delta, units=units, start=start, end=end)

def randomDatesInRange(delta, units='days', start=None, end=None):
    """
    units: seconds, minutes, hours, days
    """
    if units == 'minutes':
        delta *= 60
    if units == 'hours':
        delta *= (60 * 60)
    if units == 'days':
        delta *= (60 * 60 * 24)
    
    # random time between now and 2 months ago
    end = np.random.randint(
        low=int((datetime.datetime.utcnow() - datetime.timedelta(days=60)).timestamp()),
        high=int(datetime.datetime.utcnow().timestamp())
    )

    start = datetime.datetime.fromtimestamp(end - delta)
    end = datetime.datetime.fromtimestamp(end)
    
    return start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ")


def getLiveSensors():
    queries = []
    base_query = f'{server}/getLiveSensors?areaModel=%s&sensorSource=%s'
    for region, source in itertools.product(regions, sources):
        query = base_query % (region, source)
        queries.append((query, False))


def getTimeAggregatedData():
    """
    Query parameters:
        startTime, endTime, function, id, sensorSource, timeInterval, applyCorrection, groupBy
    """
    functions = ['mean', 'min', 'max']
    queries = []
    base_query = f'{server}/getTimeAggregatedData?'

    # basic
    # for function in functions:
    #     r, s = randomRegionAndSource()
    #     start, end = randomDateRange()
    #     q = f'{base_query}areaModel={r}&sensorSource={s}&startTime={start}&endTime={end}&function={function}'
    #     queries.append((q, True))

    # groupby
    for grouping in ['id', 'sensorSource', 'area']:
        r, s = randomRegionAndSource()
        start, end = randomDateRange()
        q = f'{base_query}startTime={start}&endTime={end}&function=mean&groupby={grouping}'
        queries.append((q, True))
    
    return queries


def getLocalSensorData():
    queries = []

def getSensorData():
    queries = []


def executeQueries(query_list, head=10):
    for q in query_list:
        
        # If it's a tuple the 2nd param is whether to print some of the result
        if isinstance(q, tuple):
            q, status = q
        else:
            status = False

        print(q)
        req = urllib.request.Request(q)
        try:
            r = urllib.request.urlopen(req, context=myssl)
        except urllib.error.URLError as e:
            print(e.reason)
        else:
            json_data = json.loads(r.read())
            objstr = json.dumps(json_data, sort_keys=True, indent=4)
            if not isinstance(json_data, list): 
                print(bc.r('ERROR 1:'), objstr)
            elif isinstance(json_data, list) and len(json_data) == 0:
                print(bc.o('EMPTY'))
            elif isinstance(json_data, list):
                print(bc.g('OK'), len(json_data))
            else:
                print(bc.r('ERROR 2:'), objstr)
            if status:
                print('\n'.join(objstr.split('\n')[:head]))


query_list = [
# f'{server}/getTimeAggregatedData?areaModel=Salt_Lake_City&startTime=2020-02-01T00:00:00Z&endTime=2020-02-02T00:00:00Z&function=mean&sensorSource=AirU',
# f'{server}/getEstimatesForLocations?lat=40.770631,40.780631&lon=-111.872235,-111.882235&startTime=2019-01-04T00:08:00Z&endTime=2019-01-04T10:00:00Z&timeInterval=0.25',
# f'{server}/getLiveSensors?areaModel=Salt_Lake_City&sensorSource=AirU',
# f'{server}/getEstimateMap?areaModel=Salt_Lake_City&time=now&latLo=40.7&latHi=40.8&lonLo=-111.8&lonHi=-111.9&latSize=20&lonSize=20',
# f'{server}/getEstimatesForLocations?lat=40.770631,40.780631&lon=-111.872235,-111.882235&startTime=2019-01-04T00:08:00Z&endTime=2019-01-04T10:00:00Z&timeInterval=0.25',
# f'{server}/getEstimatesForLocation?lat=40.780421&lon=-111.906754&startTime=2021-01-26T10:00:00Z&&endTime=2021-01-26T12:00:00Z&timeInterval=0.25',
# f'{server}/getLocalSensorData?areaModel=Salt_Lake_City&startTime=2021-06-01T00:00:00Z&endTime=2021-06-01T01:00:00Z&lat=40.77&lon=-111.86&radius=10000',
]


query_list += getTimeAggregatedData()

executeQueries(query_list, head=25)
