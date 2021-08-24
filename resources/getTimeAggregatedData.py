from google.cloud import bigquery
from datetime import timedelta
from common.params import URL_PARAMS, PARAMS_HELP_MESSAGES, list_param, multi_area, function_parse, groupby_parse
from flask_restful import Resource
from flask_restful.reqparse import RequestParser 
from flask_restful.inputs import datetime_from_iso8601
from flask import jsonify
import common.utils
import common.jsonutils
import json

# from common.decorators import processPreRequest

arguments = RequestParser()
arguments.add_argument(URL_PARAMS.START_TIME,       type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.START_TIME,         required=True)
arguments.add_argument(URL_PARAMS.END_TIME,         type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.END_TIME,           required=True)
arguments.add_argument(URL_PARAMS.FUNCTION,         type=function_parse,               help=PARAMS_HELP_MESSAGES.FUNCTION,           required=True)
arguments.add_argument(URL_PARAMS.GROUP_BY,         type=groupby_parse,                help=PARAMS_HELP_MESSAGES.GROUP_BY,           required=False, default=None)
arguments.add_argument(URL_PARAMS.TIME_INTERVAL,    type=int,                          help=PARAMS_HELP_MESSAGES.TIME_INTERVAL,      required=False, default=60)
arguments.add_argument(URL_PARAMS.SENSOR_SOURCE,    type=str,                          help=PARAMS_HELP_MESSAGES.SENSOR_SOURCE,      required=False, default="all")
arguments.add_argument(URL_PARAMS.ID,               type=list_param,                   help=PARAMS_HELP_MESSAGES.ID,                 required=False, default="all")
arguments.add_argument(URL_PARAMS.APPLY_CORRECTION, type=bool,                         help=PARAMS_HELP_MESSAGES.APPLY_CORRECTION,   required=False, default=False)
arguments.add_argument(URL_PARAMS.AREA_MODEL,       type=multi_area,                   help=PARAMS_HELP_MESSAGES.AREA_MODEL_AS_LIST, required=False, default=multi_area("all"))

class getTimeAggregatedData(Resource):

    def get(self, **kwargs):
        
        # this is used to convert the parameter terms to those used in the database
        group_tags = {"id":"ID", "sensorModel":"sensormodel", "area":"areamodel"}
        
        args = arguments.parse_args()
        
        start = args[URL_PARAMS.START_TIME]
        end = args[URL_PARAMS.END_TIME]
        function = args[URL_PARAMS.FUNCTION]
        group_by = args[URL_PARAMS.GROUP_BY]
        timeInterval = args[URL_PARAMS.TIME_INTERVAL]
        sensor_source = args[URL_PARAMS.SENSOR_SOURCE]
        id = args[URL_PARAMS.ID]
        apply_correction = args[URL_PARAMS.APPLY_CORRECTION]
        areas = args[URL_PARAMS.AREA_MODEL]

        _area_models = common.jsonutils.getAreaModels()

        if group_by in group_tags:
            group_string = f", {group_tags[group_by]}"
        else:
            group_string = ""

        # if you are going to apply correction, you need to have data grouped by area and sensortype
        if apply_correction:
            if group_by == "id" or group_by == None:
                group_string = ", " + ", ".join(list(group_tags.values())[0:3])
            elif group_by == "sensorModel":
                group_string = ", ".join([group_string, group_tags["area"]])
            elif group_by == "area":
                group_string = ", ".join([group_string, group_tags["sensorModel"]])
            else:
                group_string = ""

        SQL_FUNCTIONS = {
            "mean": "AVG",
            "min": "MIN",
            "max": "MAX",
        }

        # Check aggregation function is valid
        if function not in SQL_FUNCTIONS:
            msg = f"function is not in {SQL_FUNCTIONS.keys()}"
            return msg, 400

        time_tmp = end
        end_interval = (time_tmp + timedelta(minutes = int(timeInterval))).strftime(common.utils.DATETIME_FORMAT)

        with open('common/db_table_headings.json') as json_file:
            db_table_headings = json.load(json_file)

        tables_list = []
        
        for this_area in areas:
            need_source_query = False
            area_model = _area_models[this_area]
    
            # this logic adjusts for the two cases, where you have different tables for each source or one table for all sources
            # get all of the sources if you need to
            source_query = ""
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
                need_source_query = True
    #            source_

            for area_id_string in sources:
                empty_query = False
                time_string = db_table_headings[area_id_string]['time']
                pm2_5_string = db_table_headings[area_id_string]['pm2_5']
                lon_string = db_table_headings[area_id_string]['longitude']
                lat_string = db_table_headings[area_id_string]['latitude']
                id_string = db_table_headings[area_id_string]['id']
                model_string = db_table_headings[area_id_string]['sensormodel']
                table_string = "telemetry.telemetry"

                # area model gets taken care of below
                column_string = ", ".join([id_string + " AS ID", time_string + " AS time", pm2_5_string + " AS pm2_5", lat_string + " AS lat", lon_string+" AS lon", model_string + " AS sensormodel"])

                if "sensorsource" in db_table_headings[area_id_string]:
                    sensor_string = db_table_headings[area_id_string]['sensorsource']
                    column_string += ", " + sensor_string + " AS sensorsource"
                    if need_source_query:
                        query = f" AND sensor_string = {sensor_source}"
                elif need_source_query:
                    # if you are looking for a particular sensor source, but that's not part of the tables info, then the query is not going to return anything
                    empty_query = True

                where_string = f"pm2_5 < {common.utils.MAX_ALLOWED_PM2_5} AND time >= @start AND time <= '{end_interval}'"
                if id != "all":
                    where_string  += " AND ID = @id"
                where_string += source_query

                    # This is to cover the case where the different regions are in the same database/table and distinguised by different labels
                if "label" in db_table_headings[area_id_string]:
                    label_string = db_table_headings[area_id_string]['label']
                    column_string += ", " + label_string + " AS areamodel"
                    if area_model != "all":
    #                    where_string += " AND " + label_string + " = " + "'" + this_area + "'"
                        where_string += " AND areamodel = " + "'" + this_area + "'"
                else:
                    column_string += ", " + "'" + this_area + "'" + " AS areamodel"

                this_query = f"""(SELECT * FROM (SELECT {column_string} FROM `{table_string}`) WHERE ({where_string}))"""
            

                if not empty_query:
                    tables_list.append(this_query)


        query = f"""
            WITH
                intervals AS (
                    SELECT
                        TIMESTAMP_ADD(@start, INTERVAL @interval * num MINUTE) AS lower,
                        TIMESTAMP_ADD(@start, INTERVAL @interval * 60* (1 + num) - 1 SECOND) AS upper
                    FROM UNNEST(GENERATE_ARRAY(0,  DIV(TIMESTAMP_DIFF(@end, @start, MINUTE) , @interval))) AS num
                )
            SELECT
                CASE WHEN {SQL_FUNCTIONS.get(function)}(pm2_5) IS NOT NULL
                    THEN {SQL_FUNCTIONS.get(function)}(pm2_5)
                    ELSE 0
                    END AS PM2_5,
                upper  {group_string}
            FROM intervals
                JOIN (
                {' UNION ALL '.join(tables_list)}
            ) sensors
                ON sensors.time BETWEEN intervals.lower AND intervals.upper
            GROUP BY upper {group_string}
            ORDER BY upper"""
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "STRING", id),
                bigquery.ScalarQueryParameter("start", "TIMESTAMP", start),
                bigquery.ScalarQueryParameter("end", "TIMESTAMP", end),
                bigquery.ScalarQueryParameter("interval", "INT64", timeInterval),
            ]
        )
        
        # Run the query and collect the result
        measurements = []
        bq_client = common.utils.getBigQueryClient()
        query_job = bq_client.query(query, job_config=job_config)
        rows = query_job.result()

        if group_string == "":
            for row in rows:
                if apply_correction:
                    new_pm2_5, status = common.jsonutils.applyCorrectionFactor(_area_models[row.areamodel]['correctionfactors'], row.upper, row.PM2_5, row.sensormodel, status=True)
                else:
                    new_pm2_5 = row.PM2_5
                    status = "Not corrected"
                new_row = {"PM2_5": new_pm2_5, "time":  (row.upper + timedelta(seconds=1)).strftime(common.utils.DATETIME_FORMAT), "Status": status}
                if id != "all":
                    new_row["id"] = id
                if sensor_source != "all":
                    new_row["Sensor source"] = sensor_source
                measurements.append(new_row)
        else:
            for row in rows:
                if apply_correction:
                    new_pm2_5, status = common.jsonutils.applyCorrectionFactor(_area_models[row.areamodel]['correctionfactors'], row.upper, row.PM2_5, row.sensormodel, status=True)
                else:
                    new_pm2_5 = row.PM2_5
                    status = "Not corrected"
                new_row = {"PM2_5": new_pm2_5, "Time": (row.upper + timedelta(seconds=1)).strftime(common.utils.DATETIME_FORMAT), "Status":status}
                if group_by != None:
                    new_row[group_by] = row[group_tags[group_by]]
    
                # if a specific ID is presented, present it's location as well
                if id != "all":
                    new_row["id"] = id
                if sensor_source != "all":
                    new_row["Sensor source"] = sensor_source
                measurements.append(new_row)

        return jsonify(measurements)