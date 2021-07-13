from aqandu import app

# This is only used when running locally. When running live, gunicorn runs
# the application.
if __name__ == '__main__':
    print('Starting locally...')
    app.run(host='0.0.0.0', port=8080, debug=True)
