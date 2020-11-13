from ignite import app
import os 

# This is only used when running locally. When running live, gunicorn runs
# the application.
if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'local/ignite.json'
    app.run(host='127.0.0.1', port=8080, debug=True)
