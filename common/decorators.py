from functools import wraps
from flask import request
from flask_restful import reqparse
from datetime import timezone, timedelta
import datetime
from urllib.parse import urlparse
import datetime
from common.db_utils import RequestAPI_DB_ACCESS
import common.api_request_key_infos
from common.params import URL_PARAMS


checkKeyDecorator = reqparse.RequestParser()
checkKeyDecorator.add_argument(URL_PARAMS.KEY, type=str, required=False, default='public')

#this is so the serviceName is homogenized
def processServiceName(requestPath):
    serviceName = request.path.lower()
    if '/' in serviceName:
        serviceName = serviceName.split('/')[-1]
    return serviceName

def getRemainingTimeToWait(requestKey, serviceName, host, hoursToRecharge):
    now = datetime.datetime.now(timezone.utc)
    time = now - timedelta(hours=hoursToRecharge, minutes=0)

    oldestRequestTime = common.db_utils.RequestAPI_DB_ACCESS.getOldestAPIRequestsSinceTime(serviceName, host, requestKey, time)
    td = now - oldestRequestTime

    secondsToRecharge = hoursToRecharge * 3600
    secondsDiff = (secondsToRecharge - td.seconds) # if we are calculating this, td.seconds will be larger than secondsToRecharge (the whole reason we're calling this function because they've made too many requests within their time limit)
    seconds = secondsDiff % 60 # calculate only seconds unit (until recharge)
    minutes = int(secondsDiff / 60) % 60 # calculate only minutes unit (until recharge)
    hours = int(secondsDiff / 3600) # calculate only hours unit (until recharge)
    return {'hours': hours, 'minutes': minutes, 'seconds': seconds, "secondsDiff": secondsDiff}

def processPreRequest(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        args = checkKeyDecorator.parse_args()
        requestKey = args[URL_PARAMS.KEY]
        serviceName = processServiceName(request.path)
        host = urlparse(request.base_url).hostname
        
        # Skip localhost completely (I don't want to store tests, and this is faster)
        if host == 'localhost' or host == '127.0.0.1':
            RequestAPI_DB_ACCESS.recordServiceRequest(serviceName, host, requestKey)
            return function(*args, **kwargs)
        
        requestKeyInfo = common.api_request_key_infos.RequestAPI_KEY_INFO_ACCESS.getAllRequestKeyInfosFromKey(requestKey)
        requestLimit = requestKeyInfo[serviceName]["Limit"]
        
        if not requestKeyInfo: # no such request key exists
            return {"error_message": "Invalid Key"}
        if serviceName not in requestKeyInfo or requestLimit == 0: # no such request key exists
            return {"error_message": "You do not have access for %s" % serviceName}

        if requestLimit > 0: # -1 is unlimited so if it is -1 we don't want to ask the DB anything (it's a waste of time)
            time = datetime.datetime.now(timezone.utc) - timedelta(hours=requestKeyInfo[serviceName]["HoursToRecharge"], minutes=0)
            numRecentRequests = RequestAPI_DB_ACCESS.getNumberOfAPIRequestsSinceTime(serviceName, host, requestKey, time)
            if numRecentRequests > requestLimit: # number of requests exceeded key Limit within HoursToRecharge time
                remainingTimeToWait = getRemainingTimeToWait(requestKey, serviceName, host, requestKeyInfo[serviceName]["HoursToRecharge"])
                return {"error_message": "Request of %i requests per %i hour(s) Limit Exceeded for this API route. Please wait %i hour(s), %i minute(s) and %i second(s) before trying again" % (requestLimit, requestKeyInfo[serviceName]["HoursToRecharge"], remainingTimeToWait['hours'], remainingTimeToWait['minutes'], remainingTimeToWait['seconds'])}
        
        RequestAPI_DB_ACCESS.recordServiceRequest(serviceName, host, requestKey)

        return function(*args, **kwargs)
    return wrapper