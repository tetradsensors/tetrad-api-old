from enum import Enum
from os import stat
import common.utils
import json
import collections.abc

from google.cloud import storage

class RequestAPI_KEY_INFO_ACCESS(str, Enum):

    LIMIT = "Limit"
    HOURS_TO_RECHARGE = "HoursToRecharge"
    DEFAULT_HOURS_TO_RECHARGE = "defaultHoursToRecharge"
    DEFAULT_LIMIT = "defaultLimit"
    USERNAME = "username"

    # this method should only be accessed by public methods
    @staticmethod
    def __putAPIRequestKeyInfoToStorage(apiRequestKeyInfo):
        try:
            configData = common.utils.getConfigData()
            client = storage.Client()
            bucket = client.get_bucket(configData['resource-bucket-name'])

            blob = bucket.blob('api_request_key_info.json')
            # take the upload outside of the for-loop otherwise you keep overwriting the whole file
            blob.upload_from_string(data=json.dumps(apiRequestKeyInfo),content_type='application/json')
        except:
            return False
        return True

    # gets all the api key request information available
    @staticmethod
    def getAllRequestKeyInfos():
        apiRequestKeyInfo = {}
        try:
            config = common.utils.getConfigData()
            client = common.utils.getStorageClient()
            bucket = client.get_bucket(config['resource-bucket-name'])
            blob = bucket.get_blob('api_request_key_info.json')
            apiRequestKeyInfo = json.loads(blob.download_as_string())
        except:
            pass
        return apiRequestKeyInfo

    @staticmethod
    def getAllRequestKeyInfosFromKey(requestKey):
        apiRequestKeyInfos = RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()
        if requestKey not in apiRequestKeyInfos:
            return {}
        return apiRequestKeyInfos[requestKey]

    # insert a request key info record 
    # @staticmethod
    # def insertRequestKeyInfoService(requestKey, serviceName, hoursToRecharge, limit):
    #     apiRequestKeyInfos = RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()
    #     serviceName = serviceName.lower()
    #     try:
    #         if requestKey not in apiRequestKeyInfos:
    #             apiRequestKeyInfos[requestKey] = {}
    #         apiRequestKeyInfos[requestKey][serviceName] = { "HoursToRecharge": hoursToRecharge, "Limit": limit}
    #         RequestAPI_KEY_INFO_ACCESS.__putAPIRequestKeyInfoToStorage(apiRequestKeyInfos)
    #         return True
    #     except:
    #         return False

    @staticmethod
    def updateRequestKeyInfo(requestKey, data):
        """
        Update data for an existing key. Cannot add new key via this method.
        """
        apiRequestKeyInfos = RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()
        try:
            if requestKey not in apiRequestKeyInfos.keys():
                return False
            
            apiRequestKeyInfos[requestKey] = common.utils.update_dict_recursive(apiRequestKeyInfos[requestKey], data)
            RequestAPI_KEY_INFO_ACCESS.__putAPIRequestKeyInfoToStorage(apiRequestKeyInfos)
            return True
        except:
            return False

    # insert a request key info record 
    @staticmethod
    def insertRequestKeyInfo(requestKey, data):
        """
        Create or replace the data for a key. 
        """
        apiRequestKeyInfos = RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()
        try:
            apiRequestKeyInfos[requestKey] = data
            RequestAPI_KEY_INFO_ACCESS.__putAPIRequestKeyInfoToStorage(apiRequestKeyInfos)
            return True
        except:
            return False

    # delete a request key info record for a service
    @staticmethod
    def deleteRequestInfoKeyServiceAccess(requestKey, serviceName):
        apiRequestKeyInfos = RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()
        serviceName = serviceName.lower()
        try:
            if requestKey in apiRequestKeyInfos and serviceName in apiRequestKeyInfos[requestKey]:
                del apiRequestKeyInfos[requestKey][serviceName]
                RequestAPI_KEY_INFO_ACCESS.__putAPIRequestKeyInfoToStorage(apiRequestKeyInfos)
            return True
        except:
            return False

    # delete a request key
    @staticmethod
    def deleteRequestInfoKeyAccess(requestKey):
        apiRequestKeyInfos = RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()
        try:
            if requestKey in apiRequestKeyInfos:
                del apiRequestKeyInfos[requestKey]
                RequestAPI_KEY_INFO_ACCESS.__putAPIRequestKeyInfoToStorage(apiRequestKeyInfos)
            return True
        except:
            return False
    
    @staticmethod
    def updateFieldsForKeys(keys, data):
        apiRequestKeyInfos = RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()
        try:
            for key in keys:
                try:
                    apiRequestKeyInfos[key] = common.utils.update_dict_recursive(apiRequestKeyInfos[key], data)
                except:
                    continue    
            RequestAPI_KEY_INFO_ACCESS.__putAPIRequestKeyInfoToStorage(apiRequestKeyInfos)
            return True
        except:
            return False

    @staticmethod 
    def updateFieldsForAllKeys(data):
        apiRequestKeyInfos = RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfos()
        return RequestAPI_KEY_INFO_ACCESS.updateFieldsForKeys(apiRequestKeyInfos.keys(), data)
