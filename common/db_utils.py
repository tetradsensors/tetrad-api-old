from enum import Enum
from datetime import datetime, time, date, timezone
import datetime
import pytz
from werkzeug.wrappers import request
import common.utils
import pickle
import pathlib
import json

from google.cloud import bigquery, storage

# class DB_PARAMS(str, Enum):
#     TIME = "time"
#     ID = "ID"
#     LAT = "Latitude"
#     LON = "Longitude"
#     PM25 = "PM2_5"
#     TEMP = "Temperature"
#     HUMIDITY = "Humidity"

#     @staticmethod
#     def getSensorSource(areaIDString):
#         if areaIDString == "AIRU_TABLE_ID":
#             return "'AirU'"
#         elif areaIDString == "PURPLEAIR_TABLE_ID":
#             return "'PurpleAir'"
#         elif areaIDString == "DAQ_TABLE_ID":
#             return "'DAQ'"
#         else:
#             return None

#     @staticmethod
#     def getSensorModel(areaIDString):
#         if areaIDString == "AIRU_TABLE_ID":
#             return "3003"
#         elif areaIDString == "PURPLEAIR_TABLE_ID":
#             return "5003"
#         elif areaIDString == "DAQ_TABLE_ID":
#             return "0000"
#         else:
#             return None

#     @staticmethod
#     def getTableName(areaIDString, production = True):
#         platformDescription = "production" if production else "development"
#         if areaIDString == "AIRU_TABLE_ID":
#             return "%s.%s.airu_stationary" % (common.utils.getConfigData()['project-id'], platformDescription)
#         elif areaIDString == "PURPLEAIR_TABLE_ID":
#             return "%s.%s.purpleair_utah" % (common.utils.getConfigData()['project-id'], platformDescription)
#         elif areaIDString == "DAQ_TABLE_ID":
#             return "%s.%s.daq" % (common.utils.getConfigData()['project-id'], platformDescription)
#         else:
#             return None


class RequestAPI_DB_ACCESS(str, Enum):
    # return the number of requests by requestKey of serviceName from host since time    
    @staticmethod
    def getNumberOfAPIRequestsSinceTime(serviceName, host, requestKey, time, production=True):
        serviceName = serviceName.lower()
        platformDescription = "production" if production else "development"
        bq_client = common.utils.getBigQueryClient()
        query = """
            Select COUNT(*) as num_requests from %s.internal.api_request_tracker
            WHERE ServiceName = @ServiceName
            AND Host = @Host
            AND RequestKey = @RequestKey
            AND Time > TIMESTAMP(@Time)
            """ % (common.utils.getConfigData()['project-id'])

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ServiceName", "STRING", serviceName),
                bigquery.ScalarQueryParameter("Host", "STRING", host),
                bigquery.ScalarQueryParameter("RequestKey", "STRING", requestKey),
                bigquery.ScalarQueryParameter("Time", "DATETIME", time)
            ]
        )

        query_job = bq_client.query(query, job_config=job_config)

        num_requests = 0
        for row in query_job:
            num_requests = row['num_requests']
        return num_requests

    # gets the oldest request in time block   
    @staticmethod
    def getOldestAPIRequestsSinceTime(serviceName, host, requestKey, time, production=True):
        serviceName = serviceName.lower()
        platformDescription = "production" if production else "development"
        bq_client = common.utils.getBigQueryClient()
        query = """
            Select Time as time from %s.internal.api_request_tracker
            WHERE ServiceName = @ServiceName
            AND Host = @Host
            AND RequestKey = @RequestKey
            AND Time > TIMESTAMP(@Time)
            ORDER BY Time
            DESC
            LIMIT 1
            """ % (common.utils.getConfigData()['project-id'])

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ServiceName", "STRING", serviceName),
                bigquery.ScalarQueryParameter("Host", "STRING", host),
                bigquery.ScalarQueryParameter("RequestKey", "STRING", requestKey),
                bigquery.ScalarQueryParameter("Time", "DATETIME", time)
            ]
        )

        query_job = bq_client.query(query, job_config=job_config)

        oldestRequestTime = None
        
        for row in query_job:
            oldestRequestTime = row['time']
        return oldestRequestTime

    # insert url request  
    @staticmethod
    def recordServiceRequest(serviceName, host, requestKey, production=True):
        serviceName = serviceName.lower()
        bq_client = common.utils.getBigQueryClient()

        table_id = "%s.internal.api_request_tracker" % (common.utils.getConfigData()['project-id'])
        rows_to_insert = [
            {
                u"ServiceName": serviceName,
                u"Host": host,
                u"RequestKey": requestKey,
                u"Time": datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            }
        ]

        errors = bq_client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
