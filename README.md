# US Ignite Webpage and Backend
These are instructions for setting up the Python Virtual Environment and frontend of the US Ignite site. This project was larely taken from [AQ&U](https://github.com/aqandu/aqandu_live_site). We use Python 3 at its latest version (on GCP) which, at the time of writing, is 3.8. These instructions assume that you have python 3.8 and pip installed locally.

## Table of Contents

1. [Development Environment Quick Start](#development-environment-quick-start)
1. [Deploying In Production](#deploying-in-production)
1. [Route Documentation](#route-documentation)

  
## Development Environment Quick Start

This project uses [virtualenv](https://virtualenv.pypa.io/en/latest/installation.html) to create a new python 3.8 interpreter specific to this project. Once virtualenv is installed, you can create a new virtual environment. On Mac OSX the command will look similar to the following:

```bash
virtualenv ~/.ve/p3.8 --python 3.8
```
After creation, the environment can be loaded:
```bash
source ~/.ve/p3.8/bin/activate
```
Now the correct packages can be installed:
```bash
pip install -r requirements.txt
```
Next we can generate the flask assets with `flask assets build`. Then you may launch the application with `python main.py`. 


## Deploying in Production

To deploy the application, you have to use the command line and the gcloud tools. Once you have the production config (from Tom) and you've set up gcloud cli with the correct default project, run the following commands:

```
cp config.production.py config.py
gcloud app deploy app.yaml
```

This will start building the containers that serve the website. You can check for a successful deployment from the app engine versions dashboard in GCP. The app usually builds and deploys within a few minutes, but sometimes, Google can be a little slow with the building.

**NOTE**: If you're getting `Error Response: [4] DEADLINE_EXCEEDED` then you need to increase the timeout for the build to 20 minutes using `gcloud config set app/cloud_build_timeout 1200`.

## Tom's Notes
- `fbconfig.json` and `fbAdminConfig.json` firebase credentials files added to `Google Secrets Manager`. `apiKey` from `fbconfig.json` is in the URL to get a session token for a user. 
