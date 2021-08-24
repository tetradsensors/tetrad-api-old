from flask import Flask 
from flask_restful import Api

from resources.getSensorData import getSensorData
from resources.getTimeAggregatedData import getTimeAggregatedData
from resources.getEstimateMap import getEstimateMap

app = Flask(__name__)
api = Api(app)

api.add_resource(getSensorData, '/api/getSensorData')
api.add_resource(getTimeAggregatedData, '/api/getTimeAggregatedData')
api.add_resource(getEstimateMap, "/api/getEstimateMap")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
