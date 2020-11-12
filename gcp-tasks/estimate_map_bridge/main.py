import os 
import logging
import datetime
import requests
from google.cloud import firestore
import config
from pprint import pprint


def _add_tags(model_data, dt_str):
    model_data['lat_lo'] = CV['LAT_LO']
    model_data['lat_hi'] = CV['LAT_HI']
    model_data['lon_lo'] = CV['LON_LO']
    model_data['lon_hi'] = CV['LON_HI']
    model_data['lat_size'] = CV['LAT_SIZE']
    model_data['lon_size'] = CV['LON_SIZE']
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

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CV["CREDENTIALS_PATH"]
    fs_col = firestore.Client().collection(CV['FIRESTORE_COLLECTION'])

    DT_QUERY_STR = (datetime.datetime.utcnow() - datetime.timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
    DT_QUERY_STR = '2020-10-10T00:00:00Z'

    URL = f"""{CV['URL_BASE']}?lat_lo={CV['LAT_LO']}&lon_lo={CV['LON_LO']}&lat_hi={CV['LAT_HI']}&lon_hi={CV['LON_HI']}&lat_size={CV['LAT_SIZE']}&lon_size={CV['LON_SIZE']}&date={DT_QUERY_STR}"""
    print(URL)
    resp = requests.get(URL)
    if resp.status_code == 200:
        model_data = dict(resp.json())
        
        model_data = _reformat_2dlist(model_data)
        pprint(model_data)

        # model_data = _add_tags(model_data, DT_QUERY_STR)
        
        print('Uploading to firestore')
        ret = fs_col.document(f'{CV["FIRESTORE_DOCUMENT_PREFIX"]}{DT_QUERY_STR}').set(model_data)
        print('Firestore return:', ret)


if __name__ == '__main__':
    CV = config.config_vars
    main('data', 'context')
