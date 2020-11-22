from dotenv import load_dotenv
import os
from flask import render_template, url_for, flash, redirect, request, jsonify
#from airquality_flask import app, bq_client, pyrebase_auth, ds_client, datastore, firebase_auth
#from airquality_flask.models import User
from ignite import app, bq_client, cache, utils, gaussian_model_utils, elevation_interpolator
import json
import time

load_dotenv()

projectId = os.getenv("PROJECTID")
datasetId = os.getenv("DATASETID")
tableId = os.getenv("TABLEID")
sensor_table = os.getenv("BIGQ_SENSOR")



# ***********************************************************
# Function: request_data_flask(d) - used to populate the map with sensors
# Called by script.js
# Parameter:
# Return: Last recorded sensor input from all sensors in the DB
# ***********************************************************
@app.route("/request_data_flask/<d>/", methods=['GET'])
def request_data_flask(d):
    sensor_list = []
    # get the latest sensor data from each sensor
    # q = ("SELECT `" + sensor_table + "`.DEVICE_ID, time, PM1, PM2_5, PM10, Latitude, Longitude, Temperature, MICS_RED, MICS_OX, Humidity, SensorModel "
    #      "FROM `" + sensor_table + "` "
    #      "INNER JOIN (SELECT DEVICE_ID, MAX(time) as maxts "
    #      "FROM `" + sensor_table + "` GROUP BY DEVICE_ID) mr "
    #      "ON `" + sensor_table + "`.DEVICE_ID = mr.DEVICE_ID AND time = maxts;")

    # Rewrite by Tom to use less memory
    q= f"""SELECT 
                ID as DeviceID, 
                time as Timestamp, 
                Latitude, 
                Longitude, 
                PM2_5
            FROM
                (
                    SELECT 
                        *, 
                        ROW_NUMBER() 
                            OVER (
                                    PARTITION BY 
                                        ID 
                                    ORDER BY 
                                        time DESC) row_num
                    FROM 
                        `{sensor_table}`
                    WHERE 
                        time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
                )
            WHERE 
                row_num = 1;
    """

    query_job = bq_client.query(q)
    rows = query_job.result()  # Waits for query to finish
    for row in rows:
        sensor_list.append(
            {
                "DeviceID": str(row.DeviceID),
                "Latitude": row.Latitude,
                "Longitude": row.Longitude,
                "Timestamp": str(row.Timestamp),
                # "PM1": row.PM1,
                "PM2_5": row.PM2_5,
                # "PM10": row.PM10,
                # "Temperature": row.Temperature,
                # "Humidity": row.Humidity,
                # "MICS_OX": row.MICS_OX,
                # "MICS_RED": row.MICS_RED,
                # "SensorModel": row.SensorModel
            })

    json_sensors = json.dumps(sensor_list, indent=4)
    print (sensor_list)
    return json_sensors




