import urllib.request
import json
import csv
import pandas as pd
import ssl
myssl = ssl.create_default_context();
myssl.check_hostname=False
myssl.verify_mode=ssl.CERT_NONE

sources = ['Tetrad', 'AQ%26U', 'PurpleAir']
regions = ['slc_ut', 'chatt_tn', 'clev_oh', 'kc_mo', 'pv_ma']

aqu_server = "https://aqandu-api-development-vtjzr2unfq-uc.a.run.app/api"
local_server = "https://127.0.0.1:8080/api"
#server = aqu_server
server = local_server

query_list = [
    f'{server}/getLiveSensors?areaModel=Salt_Lake_City&sensorSource=AirU',
f'{server}/getTimeAggregatedData?areaModel=Salt_Lake_City&startTime=2020-02-01T00:00:00Z&endTime=2020-02-02T00:00:00Z&function=mean&sensorSource=AirU',
f'{server}/getEstimatesForLocations?lat=40.770631,40.780631&lon=-111.872235,-111.882235&startTime=2019-01-04T00:08:00Z&endTime=2019-01-04T10:00:00Z&timeInterval=0.25',
f'{server}/getLiveSensors?areaModel=Salt_Lake_City&sensorSource=AirU',
f'{server}/getEstimateMap?areaModel=Salt_Lake_City&time=now&latLo=40.7&latHi=40.8&lonLo=-111.8&lonHi=-111.9&latSize=20&lonSize=20',
f'{server}/getEstimatesForLocations?lat=40.770631,40.780631&lon=-111.872235,-111.882235&startTime=2019-01-04T00:08:00Z&endTime=2019-01-04T10:00:00Z&timeInterval=0.25',
f'{server}/getEstimatesForLocation?lat=40.780421&lon=-111.906754&startTime=2021-01-26T10:00:00Z&&endTime=2021-01-26T12:00:00Z&timeInterval=0.25',
f'{server}/getLocalSensorData?areaModel=Salt_Lake_City&startTime=2021-06-01T00:00:00Z&endTime=2021-06-01T01:00:00Z&lat=40.77&lon=-111.86&radius=10000',
]

for q in query_list:
    print(q)
    req = urllib.request.Request(q)
    try:
        r = urllib.request.urlopen(req, context=myssl)
    except urllib.error.URLError as e:
        print(e.reason)
    else:
        json_data = json.loads(r.read())
        print(json.dumps(json_data, sort_keys=True, indent=4))
