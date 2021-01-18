import os 
import logging
import datetime
import requests
from google.cloud import firestore
from pprint import pprint


#########################
# Following this post:
# https://cloud.google.com/blog/products/application-development/how-to-schedule-a-recurring-python-script-on-gcp
#########################


# Globals
PROJECT_ID = "us-ignite"
FIRESTORE_COLLECTION= "model"
FIRESTORE_DOCUMENT_PREFIX= "estimate_map_"
URL_BASE= "https://us-ignite.wm.r.appspot.com/api/getEstimateMap"
LAT_LO= 40.644519
LAT_HI= 40.806852
LON_LO= -111.971465
LON_HI= -111.811118
LAT_SIZE= 10
LON_SIZE= 10


def _add_tags(model_data, dt_str):
    model_data['lat_lo'] = LAT_LO
    model_data['lat_hi'] = LAT_HI
    model_data['lon_lo'] = LON_LO
    model_data['lon_hi'] = LON_HI
    model_data['lat_size'] = LAT_SIZE
    model_data['lon_size'] = LON_SIZE
    model_data['date'] = dt_str
    print('Added tags...')
    return model_data 


def _reformat_2dlist(model_data):
    for k,v in model_data.items():
        try:
            if isinstance(v[0], list):  # we found a list of lists
                
                # List of lists is now dict of lists with row indices as keys
                #   Also, keys are converted to strings to comply with Firestore (keys must be strings)
                model_data[k] = dict(zip(map(str, range(len(v))), v))

        except TypeError:   # value wasn't supscriptable (not list of lists), just keep going
            continue

    print('Reformatted list-of-lists to dict-of-lists')
    return model_data 


def main(data, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        data (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    client = firestore.Client()
    collection = client.collection(FIRESTORE_COLLECTION)

    DT_QUERY_STR = (datetime.datetime.utcnow() - datetime.timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

    URL = f"""{URL_BASE}?lat_lo={LAT_LO}&lon_lo={LON_LO}&lat_hi={LAT_HI}&lon_hi={LON_HI}&lat_size={LAT_SIZE}&lon_size={LON_SIZE}&date={DT_QUERY_STR}"""
    print(URL)
    resp = requests.get(URL)
    if resp.status_code == 200:
        model_data = dict(resp.json())
        
        model_data = _reformat_2dlist(model_data)
        pprint(model_data)

        model_data = _add_tags(model_data, DT_QUERY_STR)
        
        print('Uploading to firestore')
        ret = collection.document(f'{FIRESTORE_DOCUMENT_PREFIX}{DT_QUERY_STR}').set(model_data)
        print('Firestore return:', ret)


if __name__ == '__main__':
    main('data', 'context')
