from flask_restful import Resource, reqparse, inputs
from flask import jsonify
from google.api_core.exceptions import ServerError
from common.params import URL_PARAMS, str_lower
import common.utils
from flask import current_app
import common.api_request_key_infos

getAllRequestKeysArgs = reqparse.RequestParser()
getAllRequestKeysArgs.add_argument(URL_PARAMS.PASSCODE, type=str, required=True)

passcode = common.utils.getConfigData()["passcode"]

class getAllRequestKeys(Resource):

    def get(self, **kwargs):
        args = getAllRequestKeysArgs.parse_args()

        if passcode != args[URL_PARAMS.PASSCODE]:
            return []

        return common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()


getRequestInfoArgs = reqparse.RequestParser()
getRequestInfoArgs.add_argument(URL_PARAMS.PASSCODE, type=str, required=True)
getRequestInfoArgs.add_argument(URL_PARAMS.KEY, type=str, required=False, default='public')
class getRequestInfo(Resource):

    def get(self, **kwargs):
        args = getRequestInfoArgs.parse_args()

        if passcode != args[URL_PARAMS.PASSCODE]:
            return []

        return common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfosFromKey(args[URL_PARAMS.KEY])


setupNewRequestKeyArgs = reqparse.RequestParser()
setupNewRequestKeyArgs.add_argument(URL_PARAMS.PASSCODE,          type=str, required=True)
setupNewRequestKeyArgs.add_argument(URL_PARAMS.KEY,               type=str, required=True)
setupNewRequestKeyArgs.add_argument(URL_PARAMS.USERNAME,          type=str, required=True)
setupNewRequestKeyArgs.add_argument(URL_PARAMS.REFERRER,          type=str, required=False)
setupNewRequestKeyArgs.add_argument(URL_PARAMS.HOURS_TO_RECHARGE, type=int, required=False)
setupNewRequestKeyArgs.add_argument(URL_PARAMS.LIMIT,             type=int, required=False)
setupNewRequestKeyArgs.add_argument(URL_PARAMS.DEFAULT_LIMIT,     type=int, required=False)
setupNewRequestKeyArgs.add_argument(URL_PARAMS.DEFAULT_HOURS_TO_RECHARGE, type=int, required=False)
class setupNewRequestKey(Resource):

    def get(self, **kwargs):
        args = setupNewRequestKeyArgs.parse_args()

        if passcode != args[URL_PARAMS.PASSCODE]:
            return []

        hoursToRecharge = 1 if args[URL_PARAMS.HOURS_TO_RECHARGE] is None else args[URL_PARAMS.HOURS_TO_RECHARGE]
        limit = 60 if args[URL_PARAMS.LIMIT] is None else args[URL_PARAMS.LIMIT]
        data = {}
        for apiRoute in current_app.url_map.iter_rules():
            if 'api_limit' not in apiRoute.endpoint and 'static' not in apiRoute.endpoint:
                serviceName = apiRoute.endpoint.lower() if '/' not in apiRoute.endpoint else apiRoute.endpoint.split('/')[-1].lower()
                data[serviceName] = {
                    "HoursToRecharge": hoursToRecharge,
                    "Limit": limit
                }
        data[URL_PARAMS.USERNAME] = args[URL_PARAMS.USERNAME]
        
        if args[URL_PARAMS.REFERRER] is not None:
            data[URL_PARAMS.REFERRER] = args[URL_PARAMS.REFERRER]
                
        common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.insertRequestKeyInfo(args[URL_PARAMS.KEY], data)
        return common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()

setRequestInfoArgs = reqparse.RequestParser()
setRequestInfoArgs.add_argument(URL_PARAMS.PASSCODE,          type=str,       required=True)
setRequestInfoArgs.add_argument(URL_PARAMS.KEY,               type=str,       required=True)
setRequestInfoArgs.add_argument(URL_PARAMS.SERVICE_NAME,      type=str_lower, required=False)
setRequestInfoArgs.add_argument(URL_PARAMS.HOURS_TO_RECHARGE, type=int,       required=False)
setRequestInfoArgs.add_argument(URL_PARAMS.LIMIT,             type=int,       required=False)
setRequestInfoArgs.add_argument(URL_PARAMS.USERNAME,          type=str,       required=False)
setRequestInfoArgs.add_argument(URL_PARAMS.REFERRER,          type=str,       required=False)
setRequestInfoArgs.add_argument(URL_PARAMS.DEFAULT_LIMIT,     type=int,       required=False)
setRequestInfoArgs.add_argument(URL_PARAMS.DEFAULT_HOURS_TO_RECHARGE, type=int, required=False)
class updateRequestInfoToKey(Resource):
    '''
    Can only update a single service at a time. 
    '''

    def get(self, **kwargs):
        args = setRequestInfoArgs.parse_args()

        if passcode != args[URL_PARAMS.PASSCODE]:
            return []

        # Build the data - key values of None are allowed and will not update the existing RequestInfoKey data. Keys of None are not allowed
        data = {}
        if args[URL_PARAMS.SERVICE_NAME] is not None:
            serviceName = args[URL_PARAMS.SERVICE_NAME].lower()
            data[serviceName] = {}
            data[serviceName]["HoursToRecharge"] = args[URL_PARAMS.HOURS_TO_RECHARGE]
            data[serviceName]["Limit"] = args[URL_PARAMS.LIMIT]
        
        data[URL_PARAMS.USERNAME] = args[URL_PARAMS.USERNAME]
        data[URL_PARAMS.REFERRER] = args[URL_PARAMS.REFERRER]
        data[URL_PARAMS.DEFAULT_LIMIT] = args[URL_PARAMS.DEFAULT_LIMIT]
        data[URL_PARAMS.DEFAULT_HOURS_TO_RECHARGE] = args[URL_PARAMS.DEFAULT_HOURS_TO_RECHARGE]
        
        # make sure duplicates are not entered
        # common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.insertRequestKeyInfoService(args[URL_PARAMS.KEY], serviceName, args[URL_PARAMS.HOURS_TO_RECHARGE], args[URL_PARAMS.LIMIT])
        common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.updateRequestKeyInfo(args[URL_PARAMS.KEY], data)
        return common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfosFromKey(args[URL_PARAMS.KEY])

addReferrerArgs = reqparse.RequestParser()
addReferrerArgs.add_argument(URL_PARAMS.PASSCODE, type=str, required=True)
addReferrerArgs.add_argument(URL_PARAMS.KEY,      type=str, required=True)
addReferrerArgs.add_argument(URL_PARAMS.REFERRER, type=str, required=True)
class addReferrerTokey(Resource):
    def get(self, **kwargs):
        args = addReferrerArgs.parse_args()

        if passcode != args[URL_PARAMS.PASSCODE]:
            return []
        
        common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.insertReferrerToExistingKey(args[URL_PARAMS.KEY], args[URL_PARAMS.REFERRER])
        return common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfosFromKey(args[URL_PARAMS.KEY])

deleteRequestInfoArgs = reqparse.RequestParser()
deleteRequestInfoArgs.add_argument(URL_PARAMS.PASSCODE,     type=str,       required=True)
deleteRequestInfoArgs.add_argument(URL_PARAMS.KEY,          type=str,       required=True)
deleteRequestInfoArgs.add_argument(URL_PARAMS.SERVICE_NAME, type=str_lower, required=True)
class deleteRequestInfoKeyServiceAccess(Resource):

    def get(self, **kwargs):
        args = deleteRequestInfoArgs.parse_args()

        if passcode != args[URL_PARAMS.PASSCODE]:
            return []
        
        serviceName = args[URL_PARAMS.SERVICE_NAME].lower()

        if args[URL_PARAMS.KEY] == 'all':
            apiRequestKeyInfos = common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()
            keys = list(apiRequestKeyInfos.keys())
        else:
            keys = [args[URL_PARAMS.KEY]]
        
        for key in keys:
            common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.deleteRequestInfoKeyServiceAccess(key, serviceName)
            
        return common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()

deleteRequestInfoKeyAccessArgs = reqparse.RequestParser()
deleteRequestInfoKeyAccessArgs.add_argument(URL_PARAMS.PASSCODE, type=str, required=True)
deleteRequestInfoKeyAccessArgs.add_argument(URL_PARAMS.KEY, type=str, required=True)
class deleteRequestInfoKeyAccess(Resource):

    def get(self, **kwargs):
        args = deleteRequestInfoKeyAccessArgs.parse_args()

        if passcode != args[URL_PARAMS.PASSCODE]:
            return []

        common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.deleteRequestInfoKeyAccess(args[URL_PARAMS.KEY])

        return common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()

updateFieldsArgs = reqparse.RequestParser()
updateFieldsArgs.add_argument(URL_PARAMS.PASSCODE,                  type=str,       required=True)
updateFieldsArgs.add_argument(URL_PARAMS.SERVICE_NAME,              type=str_lower, required=False)
updateFieldsArgs.add_argument(URL_PARAMS.HOURS_TO_RECHARGE,         type=int,       required=False)
updateFieldsArgs.add_argument(URL_PARAMS.LIMIT,                     type=int,       required=False)
updateFieldsArgs.add_argument(URL_PARAMS.USERNAME,                  type=str,       required=False)
updateFieldsArgs.add_argument(URL_PARAMS.REFERRER,                  type=str,       required=False)
updateFieldsArgs.add_argument(URL_PARAMS.DEFAULT_LIMIT,             type=int,       required=False)
updateFieldsArgs.add_argument(URL_PARAMS.DEFAULT_HOURS_TO_RECHARGE, type=int,       required=False)
class updateFieldsForAllKeys(Resource):
    def get(self, **kwargs):
        args = updateFieldsArgs.parse_args()

        if passcode != args[URL_PARAMS.PASSCODE]:
            return []

        # Build the data - key values of None are allowed and will not update the existing RequestInfoKey data. Keys of None are not allowed
        data = {}
        if args[URL_PARAMS.SERVICE_NAME] is not None:
            serviceName = args[URL_PARAMS.SERVICE_NAME].lower()
            data[serviceName] = {}
            data[serviceName]["HoursToRecharge"] = args[URL_PARAMS.HOURS_TO_RECHARGE]
            data[serviceName]["Limit"] = args[URL_PARAMS.LIMIT]
        
        data[URL_PARAMS.USERNAME] = args[URL_PARAMS.USERNAME]
        data[URL_PARAMS.REFERRER] = args[URL_PARAMS.REFERRER]
        data[URL_PARAMS.DEFAULT_LIMIT] = args[URL_PARAMS.DEFAULT_LIMIT]
        data[URL_PARAMS.DEFAULT_HOURS_TO_RECHARGE] = args[URL_PARAMS.DEFAULT_HOURS_TO_RECHARGE]
        
        common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.updateFieldsForAllKeys(data)
        return common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()

newServiceArgs = reqparse.RequestParser()
newServiceArgs.add_argument(URL_PARAMS.PASSCODE,     type=str,       required=True)
newServiceArgs.add_argument(URL_PARAMS.SERVICE_NAME, type=str_lower, required=True)
class addNewService(Resource):
    def get(self, **kwargs):
        args = newServiceArgs.parse_args()
    
        if passcode != args[URL_PARAMS.PASSCODE]:
            return []

        service_name = args[URL_PARAMS.SERVICE_NAME].lower()
        
        apiRequestKeyInfos = common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()
        
        for key in apiRequestKeyInfos.keys():
            limit = apiRequestKeyInfos[key].get(common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.DEFAULT_LIMIT, 0)
            hours_to_recharge = apiRequestKeyInfos[key].get(common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.DEFAULT_HOURS_TO_RECHARGE, 1)
            data = {
                service_name: {
                    common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.LIMIT: limit,
                    common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.HOURS_TO_RECHARGE: hours_to_recharge
                }
            }
            common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.updateRequestKeyInfo(key, data)
        return common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()