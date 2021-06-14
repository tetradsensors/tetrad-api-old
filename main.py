# This is only used when running locally. When running live, gunicorn runs
# the application.
if __name__ == '__main__':
    from os import environ
    # from dotenv import load_dotenv
    # load_dotenv(dotenv_path='local/.env', override=True)
    from tetrad import app
    
    app.run(host='127.0.0.1', port=8080, debug=True)

# Running live, gunicorn runs the application
else:
    from tetrad import app