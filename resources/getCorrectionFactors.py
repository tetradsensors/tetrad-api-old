from common.params import URL_PARAMS, PARAMS_HELP_MESSAGES, multi_area
from flask_restful import Resource
from flask_restful.reqparse import RequestParser 
from flask_restful.inputs import datetime_from_iso8601
from flask import jsonify
import common.utils
import common.jsonutils

from common.decorators import processPreRequest

arguments = RequestParser()
arguments.add_argument(URL_PARAMS.TIME,       type=datetime_from_iso8601, help=PARAMS_HELP_MESSAGES.TIME,               required=False, default=None)
arguments.add_argument(URL_PARAMS.AREA_MODEL, type=multi_area,            help=PARAMS_HELP_MESSAGES.AREA_MODEL_AS_LIST, required=False, default=multi_area("all"))

class getCorrectionFactors(Resource):

    @processPreRequest
    def get(self, **kwargs):
        args = arguments.parse_args()
        time = args[URL_PARAMS.TIME]
        areas = args[URL_PARAMS.AREA_MODEL]

        _area_models = common.jsonutils.getAreaModels()

        all_factors = {}
        for area in areas:
            area_model = _area_models[area]
            factors = area_model['correctionfactors']

            if time != None:
                area_factors = {}
                this_time = common.jsonutils.parseDateString(time, area_model['timezone'])
                for this_type in factors:
                    for i in range(len(factors[this_type])):
                        if (factors[this_type][i]['starttime'] <= this_time and factors[this_type][i]['endtime'] > this_time) or (factors[this_type][i]['starttime'] == "default"):
                            area_factors[this_type] = factors[this_type][i]
                            break
                all_factors[area] = area_factors
            else:
                all_factors[area] = factors

        return jsonify(all_factors) 