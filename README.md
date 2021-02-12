# Tetrad Backend
These are instructions for setting up the Python Virtual Environment and frontend of the US Ignite site. This project was larely adapted from [AQ&U](https://github.com/aqandu/aqandu_live_site). We use Python 3 at its latest version (on GCP) which, at the time of writing, is 3.8. These instructions assume that you have python 3.8 and pip installed locally.

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
gcloud app deploy app.yaml
```

For additional information, include the `verbosity` flag:

```
gcloud app deploy --verbosity debug app.yaml
```

Also, a fuller command:

```bash
gcloud app deploy --appyaml app.yaml --verbosity debug --no-cache --promote
```

This will start building the containers that serve the website. You can check for a successful deployment from the app engine versions dashboard in GCP. The app usually builds and deploys within a few minutes, but sometimes, Google can be a little slow with the building.

**NOTE**: If you're getting `Error Response: [4] DEADLINE_EXCEEDED` then you need to increase the timeout for the build to 20 minutes using 

```bash
gcloud config set app/cloud_build_timeout 1200
```

-----------------

## Tom's Notes

### Flask
- Logging can be used, but the API must be enabled. Then the service account (`tetrad-296715@appspot.gserviceaccount.com`) needs the permissions: `Logging Admin`
- `import logging`
  - `logging.debug('This is a debug message')`
  - `logging.info('This is an info message')`
  - `logging.warning('This is a warning message')`
  - `logging.error('This is an error message')`
  - `logging.critical('This is a critical message')`
- `@ingroup(<group[s] [String/List]>)` decorator: Group or groups which can run this route. See "Firestore" for more details

### Firebase
- Create new project in [Firebase](https://console.firebase.google.com), which comes from your GCP project
- `Authentication`: Enable Email/Password, Google
- "Public-facing project name": `Tetrad Sensor Networks, LLC`
- Download the `Firebase Config`JSON
  - In Firebase: create a web app, then Settings -> General -> Web App -> Firebase SDK snippet (Config) -> copy and paste to JSON file and reformat as a valid JSON object.
- `apiKey` from `fbconfig.json` is in the URL to get a session token for a user. Will only work if attached to a valid service account (which GAE instances are, by default).
- `fbconfig.json` is stored in [Google Secret Manager](https://console.cloud.google.com/security/secret-manager?project=tetrad-296715&folder=&organizationId=) as `firebase_config`.
- Once you have a JWT token, add it to the the `Authentication` Header when you make your request
- All API endpoints use HTTP `POST`, and require the `Authentication` header, so they will not work through the browser's address input. 

### Firestore
- Collection `user_groups`: Each document is a group (name of document)
  - Group (document) names: `airuv2`, `admin`, `slc`, `chatt`, `ks`, `clev`
  - Field key `uids` is array of Firebase User UIDs with access in this group

### Secret Manager
- First, enable the API
- Add a new file:
```bash
gcloud secrets create "secret_name_on_server" --data-file="/path/to/file"
```
- Update GAE service account permissions: `Secret Manager Secret Accessor`

### Service Accounts
- Default app engine service account is used to validate all APIs. `tetrad-296715@appspot.gserviceaccount.com`

### BigQuery


### App Engine
- Can attach a service to a subdomain through the `dispatch.yaml` file
- in `app.yaml` include line: `service: <service-name>` to create a named service
- There is a `default` service running. It's not possible to delete this service because GAE requires it internally. I've deployed an empty project to the service in a  `Standard Environment` with no web server, so it's impossible to communicate with this service. It's domain is `tetrad-296715.wm.r.appspot.com`, which is given automatically by GAE to the default service, and this domain cannot be changed. 
- GAE will automatically cache file responses if you don't include an `Authorization` header

### Google Cloud Functions


### SSL & Load Balancer
- SSL certificates are managed by Google, using Let's Encrpyt as the Certificate Authority. Certificates are auto-renewing. 
- If we want/need to add a load balancer, I think we need to do custom SSL certs.
- Subdomain of `ota.tetradsensors.com` holds routes for airu file download, namely new certificates and firmware updates. 
- In GAE, `ota.tetradsensors.com` was registered. "Disable managed security" was selected. Custom SSL Certificate was created with procedure outlined by Espressif in the project `esp-idf/examples/system/ota/README.md`. The procedure is as follows:

```bash
openssl req -x509 -newkey rsa:2048 -keyout ca_key.pem -out ca_cert.pem -days 3650 -node
```
  - Country Name (2 letter code) []:`US`
  - State or Province Name (full name) []:`Utah`
  - Locality Name (eg, city) []:`Salt Lake City`
  - Organization Name (eg, company) []:`Tetrad Sensor Network Solutions, LLC`
  - Organizational Unit Name (eg, section) []:`com`
  - Common Name (eg, fully qualified host name) []:`ota.tetradsensors.com`
  - Email Address []:`contact@tetradsensors.com`

Then the private key needs to be converted to an RSA Private Key for GAE to accept it.:

```bash
openssl rsa -in ca_key.pem -out ca_key_rsa.pem
```
- Then they can be uploaded to GAE->Settings->SSL certificates->Upload a new certificate and apply the cert to the subdomain `ota.tetradsensors.com`

### Domain and DNS Registrar
- we are hosted through Google Domains
- \[sub\]domains are currently: `tetradsensors.com`, `www.tetradsensors.com`, `api.tetradsensors.com`

### OTA
- Subdomain `ota.tetradsensors.com`
- All routes wrapped in `@check_creds`
  - all AirUs use the same <username:password> in `Authorization` header 

### New City Checklist
- Update `model_boxes.json` in GS
- Update `app.yaml`
- Update `app_consts.py`
