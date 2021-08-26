from google.cloud import bigquery
from common.params import URL_PARAMS, PARAMS_HELP_MESSAGES, list_param, multi_area, bool_flag
from flask_restful import Resource
from flask_restful.reqparse import RequestParser 
from flask_restful.inputs import datetime_from_iso8601
from flask import jsonify
import common.utils
import common.jsonutils
import json

from common.decorators import processPreRequest

arguments = RequestParser()
arguments.add_argument(URL_PARAMS.START_TIME,    type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.START_TIME,         required=True)
arguments.add_argument(URL_PARAMS.END_TIME,      type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.END_TIME,           required=True)
arguments.add_argument(URL_PARAMS.SENSOR_SOURCE, type=str,                   help=PARAMS_HELP_MESSAGES.SENSOR_SOURCE,      required=False, default="all")
arguments.add_argument(URL_PARAMS.ID,            type=list_param,            help=PARAMS_HELP_MESSAGES.ID,                 required=False, default=None)
arguments.add_argument(URL_PARAMS.NO_CORRECTION, type=bool_flag,             help=PARAMS_HELP_MESSAGES.NO_CORRECTION,      required=False, default=False)
arguments.add_argument(URL_PARAMS.AREA_MODEL,    type=multi_area,            help=PARAMS_HELP_MESSAGES.AREA_MODEL_AS_LIST, required=False, default=multi_area("all"))

class getSensorData(Resource):

    @processPreRequest
    def get(self, **kwargs):
        args = arguments.parse_args()
        id = args[URL_PARAMS.ID]
        sensor_source = args[URL_PARAMS.SENSOR_SOURCE]
        start = args[URL_PARAMS.START_TIME]
        end = args[URL_PARAMS.END_TIME]
        apply_correction = not bool(args[URL_PARAMS.NO_CORRECTION])
        areas = args[URL_PARAMS.AREA_MODEL]

        # Download and parse area_params.json
        _area_models = common.jsonutils.getAreaModels()

        with open('common/db_table_headings.json') as json_file:
            db_table_headings = json.load(json_file)
            
        query_list = []

        for this_area in areas:
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
                source_query = f" AND sensorsource = @sensor_source"

            # area_id_string = "TETRAD_TABLE_ID" always
            for area_id_string in sources:
                empty_query = False
                time_string = db_table_headings[area_id_string]['time']
                pm2_5_string = db_table_headings[area_id_string]['pm2_5']
                lon_string = db_table_headings[area_id_string]['longitude']
                lat_string = db_table_headings[area_id_string]['latitude']
                id_string = db_table_headings[area_id_string]['id']
                model_string = db_table_headings[area_id_string]['sensormodel']
                table_string = "telemetry.telemetry"

                column_string = " ".join([id_string, "AS ID,", time_string, "AS time,", pm2_5_string, "AS pm2_5,", lat_string, "AS lat,", lon_string, "AS lon,", model_string, "AS sensormodel"])
                
                # put together a separate query for all of the specified sources
                if "sensorsource" in db_table_headings[area_id_string]:
                    sensor_string = db_table_headings[area_id_string]['sensorsource']
                    column_string += ", " + sensor_string + " AS sensorsource"
                elif (not source_query==""):
                # if you are looking for a particular sensor source, but that's not part of the tables info, then the query is not going to return anything
                    empty_query = True


                    # for efficiency, don't do the query if the sensorsource is needed by not available

                where_string = "time >= @start AND time <= @end"
                if id != None:
                    where_string  += " AND ID = @id"
                where_string += source_query

                if "label" in db_table_headings[area_id_string]:
                    label_string = db_table_headings[area_id_string]['label']
                    column_string += ", " + label_string + " AS area_model"
                    if area_model != "all":
                        where_string += " AND " + "area_model" + " = " + "'" + this_area + "'"
                else:
                    column_string += ", " + "'" + this_area + "'" + " AS area_model"

                this_query = f"""(SELECT * FROM (SELECT {column_string} FROM `{table_string}`) WHERE ({where_string}))"""

                if not empty_query:
                    query_list.append(this_query)

            query = " UNION ALL ".join(query_list) + " ORDER BY time ASC "

        job_config = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("id", "STRING", id),
                bigquery.ScalarQueryParameter("sensor_source", "STRING", sensor_source),
                bigquery.ScalarQueryParameter("start", "TIMESTAMP", start),
                bigquery.ScalarQueryParameter("end", "TIMESTAMP", end),
            ])

        # Run the query and collect the result
        measurements = []
        bq_client = common.utils.getBigQueryClient()
        query_job = bq_client.query(query, job_config=job_config)
        #    rows = query_job.result()
        df = query_job.to_dataframe()
        status_data = ["No correction"]*df.shape[0]
        df["status"] = status_data

        #    df.append(bigquery.SchemaField("status", "STRING"))
        # dev.dev: 1224199135
        # telemet: 1224327842
        #    rows.append(bigquery.SchemaField("status", "STRING"))
        # apply correction factors unless otherwise noted
        if apply_correction:
            for idx, datum in df.iterrows():
                df.at[idx, 'pm2_5'], df.at[idx, 'status'] = common.jsonutils.applyCorrectionFactor2(_area_models[datum["area_model"]]['correctionfactors'], datum, status=True)
                
        #    else:
        #        datum['status'] = "No correction"

        df = df.fillna('null')

        for idx, row in df.iterrows():
            measurements.append({"Sensor source": row["sensorsource"], "Sensor ID": row["ID"], "PM2_5": row["pm2_5"], "Time": row["time"].strftime(common.utils.DATETIME_FORMAT), "Latitude": row["lat"], "Longitude": row["lon"], "Status": row["status"]})
        
        
        # tags = [{
        #     "ID": id,
        #     "SensorSource": sensor_source,
        #     "time": datetime.utcnow().strftime(utils.DATETIME_FORMAT)
        # }]
        # return jsonify({"data": measurements, "tags": tags})
        return jsonify(measurements)

            