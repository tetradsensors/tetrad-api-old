from flask import Flask 
from flask_restful import Api

from resources.getSensorData import getSensorData
from resources.getTimeAggregatedData import getTimeAggregatedData
from resources.getEstimateMap import getEstimateMap
from resources.getLiveSensors import getLiveSensors
from resources.getCorrectionFactors import getCorrectionFactors
from resources.getLocalSensorData import getLocalSensorData
from resources.getEstimateAtLocation import getEstimateAtLocation
from resources.getEstimateAtLocations import getEstimateAtLocations
from resources.getBoundingBox import getBoundingBox

from resources.requestKeyInfo import getAllRequestKeys
from resources.requestKeyInfo import getRequestInfo
from resources.requestKeyInfo import updateFieldsForAllKeys
from resources.requestKeyInfo import setupNewRequestKey
from resources.requestKeyInfo import updateRequestInfoToKey
from resources.requestKeyInfo import deleteRequestInfoKeyAccess
from resources.requestKeyInfo import deleteRequestInfoKeyServiceAccess
from resources.requestKeyInfo import addNewService

app = Flask(__name__)
api = Api(app)

api.add_resource(getSensorData,          '/api/getSensorData')
api.add_resource(getTimeAggregatedData,  '/api/getTimeAggregatedData')
api.add_resource(getEstimateMap,         '/api/getEstimateMap')
api.add_resource(getLiveSensors,         '/api/getLiveSensors')
api.add_resource(getCorrectionFactors,   '/api/getCorrectionFactors')
api.add_resource(getLocalSensorData,     '/api/getLocalSensorData')
api.add_resource(getEstimateAtLocation,  '/api/getEstimateAtLocation')
api.add_resource(getEstimateAtLocations, '/api/getEstimateAtLocations')
api.add_resource(getBoundingBox,         '/api/getBoundingBox')

api.add_resource(getAllRequestKeys,                 '/api_limited/getAllRequestKeys')
api.add_resource(getRequestInfo,                    '/api_limited/getRequestInfo')
api.add_resource(setupNewRequestKey,                '/api_limited/setupNewRequestKey')
api.add_resource(updateRequestInfoToKey,            '/api_limited/updateRequestInfoToKey')
api.add_resource(updateFieldsForAllKeys,            '/api_limited/updateFieldsForAllKeys')
api.add_resource(deleteRequestInfoKeyAccess,        '/api_limited/deleteRequestInfoKeyAccess')
api.add_resource(deleteRequestInfoKeyServiceAccess, '/api_limited/deleteRequestInfoKeyServiceAccess')
api.add_resource(addNewService,                     '/api_limited/addNewService')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
