from google.cloud import bigquery
from datetime import datetime, timedelta
from common.params import URL_PARAMS, PARAMS_HELP_MESSAGES, multi_area, bool_flag
from flask_restful import Resource
from flask_restful.reqparse import RequestParser 
from flask import jsonify
import numpy as np
import common.utils
import common.jsonutils
import json

from common.decorators import processPreRequest

arguments = RequestParser()
arguments.add_argument(URL_PARAMS.SENSOR_SOURCE, type=str,        help=PARAMS_HELP_MESSAGES.SENSOR_SOURCE,      required=False, default="all")
arguments.add_argument(URL_PARAMS.NO_CORRECTION, type=bool_flag,  help=PARAMS_HELP_MESSAGES.NO_CORRECTION,      required=False, default=False)
arguments.add_argument(URL_PARAMS.AREA_MODEL,    type=multi_area, help=PARAMS_HELP_MESSAGES.AREA_MODEL_AS_LIST, required=False, default=multi_area("all"))
arguments.add_argument(URL_PARAMS.FLAG_OUTLIERS, type=bool_flag,  help=PARAMS_HELP_MESSAGES.FLAG_OUTLIERS,      required=False, default=False)

class getLiveSensors(Resource):

    @processPreRequest
    def get(self, **kwargs):
        args = arguments.parse_args()
        sensor_source = args[URL_PARAMS.SENSOR_SOURCE]
        apply_correction = not args[URL_PARAMS.NO_CORRECTION]
        areas = args[URL_PARAMS.AREA_MODEL]
        flag_outliers = bool(args[URL_PARAMS.FLAG_OUTLIERS])

        # Download and parse area_params.json
        _area_models = common.jsonutils.getAreaModels()

        # check if sensor_source is specified
        # If not, default to all
        if sensor_source == "" or sensor_source == "undefined" or sensor_source==None:
            # Check that the arguments we want exist
            sensor_source = "all"

        # Define the BigQuery query
        now = datetime.utcnow()
        one_hour_ago = now - timedelta(hours=1)  # AirU + PurpleAir sensors have reported in the last hour
        query_list = []

        with open('common/db_table_headings.json') as json_file:
            db_table_headings = json.load(json_file)
            
        query_list = []

        for this_area in areas:
            need_source_query = False
            area_model = _area_models[this_area]
    #        print(area_model)
            # this logic adjusts for the two cases, where you have different tables for each source or one table for all sources
            # get all of the sources if you need to
            source_query = "TRUE"
            if (sensor_source == "all"):
                # easy case, query all tables with no source requirement
                sources = area_model["idstring"]
            elif "sourcetablemap" in area_model:
                # if it's organized by table, then get the right table (or nothing)
                if sensor_source in area_model["sourcetablemap"]:
                    sources = area_model["sourcetablemap"][sensor_source]
                else:
                    sources = None
            else:
                # sources are not organized by table.  Get all the tables and add a boolean to check for the source
                sources = area_model["idstring"]
    #            source_query = f"{sensor_string} = @sensor_source"
                need_source_query = True

            for area_id_string in sources:
                where_string = " WHERE TRUE"
                empty_query = False
                time_string = db_table_headings[area_id_string]['time']
                pm2_5_string = db_table_headings[area_id_string]['pm2_5']
                lon_string = db_table_headings[area_id_string]['longitude']
                lat_string = db_table_headings[area_id_string]['latitude']
                id_string = db_table_headings[area_id_string]['id']
                model_string = db_table_headings[area_id_string]['sensormodel']
                table_string = "telemetry.telemetry"

                column_string = ", ".join([id_string + " AS ID", time_string + " AS time", pm2_5_string + " AS pm2_5", lat_string + " AS lat", lon_string+" AS lon", model_string + " AS sensormodel"])
                # put together a separate query for all of the specified sources
                group_string = ", ".join(["ID", "pm2_5", "lat", "lon", "area_model", "sensormodel"])

                if "sensorsource" in db_table_headings[area_id_string]:
                    sensor_string = db_table_headings[area_id_string]['sensorsource']
                    column_string += ", " + sensor_string + " AS sensorsource"
                    group_string += ", sensorsource"
                    if need_source_query:
                        source_query = f"{sensor_string} = '{sensor_source}'"
                elif need_source_query:
                    # if you are looking for a particular sensor source, but that's not part of the tables info, then the query is not going to return anything
                    empty_query = True

                    # This is to cover the case where the different regions are in the same database/table and distinguised by different labels
                if "label" in db_table_headings[area_id_string]:
                    label_string = db_table_headings[area_id_string]['label']
                    column_string += ", " + label_string + " AS area_model"
                    if area_model != "all":
                        where_string += " AND " + label_string + " = " + "'" + this_area + "'"

                else:
                    column_string += ", " + "'" + this_area + "'" + " AS area_model"

                where_string += f" AND {source_query} AND {time_string} >= '{str(one_hour_ago)}'"

                this_query = f"""(WITH a AS (SELECT {column_string} FROM `{table_string}` {where_string}),  b AS (SELECT {id_string} AS ID, max({time_string})  AS LATEST_MEASUREMENT FROM `{table_string}` WHERE {time_string} >= '{str(one_hour_ago)}' GROUP BY {id_string}) SELECT * FROM a INNER JOIN b ON a.time = b.LATEST_MEASUREMENT and b.ID = a.ID)"""

                if not empty_query:
                    query_list.append(this_query)

        # Build the actual query from the list of options
        query = " UNION ALL ".join(query_list)
        # Run the query and collect the result

        bq_client = common.utils.getBigQueryClient()
        query_job = bq_client.query(query)
    
        df = query_job.to_dataframe()
        status_data = [[]]*df.shape[0]
        df["status"] = status_data
        
        if flag_outliers:
            filters = {}
            for this_area in areas:
                area_model = _area_models[this_area]
                lo_filter, hi_filter = common.utils.filterUpperLowerBoundsForArea(str(one_hour_ago), str(now), area_model)
                filters[this_area] = (lo_filter,hi_filter)
            for idx, datum in df.iterrows():
                this_lo, this_hi = filters[datum["area_model"]]
                this_data = datum['pm2_5']
                if  this_data < 0.0:
                    df.at[idx, 'status'] = df.at[idx, 'status'] + ["No data"]
                elif (this_data < this_lo) or (this_data > this_hi) or np.isnan(this_data):
                    df.at[idx, 'status'] = df.at[idx, 'status'] + ["Outlier"]

        if apply_correction:
            for idx, datum in df.iterrows():
                df.at[idx, 'pm2_5'], this_status= common.jsonutils.applyCorrectionFactor2(_area_models[datum["area_model"]]['correctionfactors'], datum, status=True)
                df.at[idx, 'status'] = df.at[idx, 'status'] + [this_status]
        else:
            for idx, datum in df.iterrows():
                df.at[idx, 'status'] = df.at[idx, 'status'] + ["No correction"]

        sensor_list = []

        df = df.fillna('null')
        
        for idx, row in df.iterrows():
            sensor_list.append(
                {
                    "Sensor ID": str(row["ID"]),
                    "Latitude": row["lat"],
                    "Longitude": row["lon"],
                    "Time": row["time"],
                    "PM2_5": row["pm2_5"],
                    "Sensor model": row["sensormodel"],
                    "Sensor source": row["sensorsource"],
                    "Status":row["status"]
                }
            )
        
        return jsonify(sensor_list)

            