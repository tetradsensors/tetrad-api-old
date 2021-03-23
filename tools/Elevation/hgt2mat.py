# ---------------------------------------------------------------------
# Script Usage:
# 
# Download the desired .HGT files from the following URL
# and place them into a folder. All files in a folder will be tiled
# into a single 2D numpy array of elevation data. Change the user 
# variables as needed then run the script. 
# 
# Obtaining elevation data (.HGT) from NASA:
# https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/
# ---------------------------------------------------------------------

import os
import math 
import numpy as np
from scipy.io import savemat
from scipy.ndimage.measurements import label
import numpy as np
from glob import glob
import json 

# ---------------------------------------------------------------------
# USER Variables
# ---------------------------------------------------------------------
# Location of json file specifying GPS bounds for each city
GPS_BOUNDS_FILE = 'model_boxes.json'

# location HGT files for tiling
HGT_FILES_DIR = 'LPDAAC/chatt'

# my model box is set up as an array of dictionaries. Yours may
# be different and your function "getModelBoxRegion()" will need
# to be changed
MODEL_BOX_REGION_NAME = 'CHATT'

# Path of output .mat file. 
MAT_OUT = f'mats/{MODEL_BOX_REGION_NAME}_elevations.mat'
# ---------------------------------------------------------------------


def _getModelBoxesRemote():
    from google.cloud import storage
    os.environ['GOOGLE_APPLICATIONS_CREDENTIALS'] = None
    gs_client = storage.Client()
    bucket = gs_client.get_bucket("tetrad_server_files")
    blob = bucket.get_blob("model_boxes.json")
    return json.loads(blob.download_as_string())

def _getModelBoxesLocal(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def getModelBoxes(local_filename=None, remote=False):
    '''
    Get file and convert into dictionary. 
    Set 'local_filename' to grab from local file. 
    If 'remote' is true, grab from Google Cloud Platform file.
    '''
    assert local_filename or remote, "Specify one of 'local_filename' or 'remote'" 
    
    if remote:
        return _getModelBoxesRemote()
    else:
        return _getModelBoxesLocal(local_filename)

def getModelBoxRegion(model_boxes, region_name):
    '''
    Change this function depending on format of
    your model boxes json file. 
    Return: the region of interest
    '''
    for region in model_boxes:
        if region['qsrc'] == region_name: 
            return region 
    assert False, "No region found"
    return None 


def find_nearest(array, value):
    '''
    Return index in array of value closest to 'value'. 
    For array with identical values, will return index
    of first to appear in array. Shouldn't be relevant
    in this application
    '''
    array = np.asarray(array)
    return (np.abs(array - value)).argmin()


# https://stackoverflow.com/questions/357415/how-to-read-nasa-hgt-binary-files
def hgt2Mat(hgt_filename):
    '''
    Files obtained from:
    https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/
    are received as .HGT. This function converts them
    into a numpy nd-array. 
    '''
    siz = os.path.getsize(hgt_filename)
    dim = int(math.sqrt(siz / 2))
    assert dim * dim * 2 == siz, "Invalid file size"
    return np.fromfile(hgt_filename, np.dtype('>i2'), dim * dim).reshape((dim, dim))


# https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/
def tileHGT(hgt_list):
    '''
    Take in a list of .HGT files ('hgt_list') and 
    tile them into a numpy array. 
    
    Return a dictionary of the form:
    {
        'lats': list of latitudes
        'lons': list of longitudes
        'mat': 2D numpy elevation (meters)
    }
    '''
    SZ = 3600 # defined by NASA site, user: don't change
    lat_lo, lon_lo =  1000,  1000
    lat_hi, lon_hi = -1000, -1000
    file_info = []
    cnt = 0

    # Loop through the .HGT file names. Store them in individual
    # and find the upper/lower lat/lon bounds.
    for fn in hgt_list:
        f = fn.split('/')[-1].split('.hgt')[0]
        if 'W' in f:
            lats, lon = f.split('W')
            lon = int(lon) * -1
        elif 'E' in f:
            lats, lon = f.split('E')
            lon = int(lon)

        assert lats and lon, "Bad filename. Must be something like: N40W110.hgt"

        lat = int(lats[1:])
        if 'S' in lats:
            lat *= -1

        lat_lo = min(lat_lo, lat)
        lon_lo = min(lon_lo, lon)
        lat_hi = max(lat_hi, lat)
        lon_hi = max(lon_hi, lon)

        file_info.append({
            'filename': fn,
            'lat': lat,
            'lon': lon
        })

    print('latlo:', lat_lo, 'lathi:', lat_hi, 'lonlo:', lon_lo, 'lonhi:', lon_hi)

    # Use the newly found upper/lower lat/lon bounds to 
    # create a 2D numpy array (0's) with the correct size
    szX = (((lon_hi + 1) - lon_lo) * SZ) + 1
    szY = (((lat_hi + 1) - lat_lo) * SZ) + 1
    canvas = np.zeros((szY, szX))

    # Now go back through the matrices and add them
    # to the correct location of the canvas
    for f in file_info:
        x0 = (f['lon'] - lon_lo) * SZ
        # y0 = (f['lat'] - lat_lo) * SZ
        y0 = (lat_hi - f['lat']) * SZ
        print('lat:', f['lat'], 'y:', 'lat_lo:', lat_lo, 'lat_hi:', lat_hi)
        canvas[y0:y0+SZ+1, x0:x0+SZ+1] = hgt2Mat(f['filename'])

    # Compute the X-Y values
    lon_locs = np.linspace(lon_lo, lon_hi + 1, szX)
    lat_locs = np.linspace(lat_lo, lat_hi + 1, szY)

    return {
        "lats": lat_locs,
        "lons": lon_locs,
        "mat": canvas
    }


def extractRegion(data_dict, lat_lo, lat_hi, lon_lo, lon_hi):
    '''
    Crop the elevation matrix by the prescribed lat/lon bounds
    Return a dictionary of the form:
    {
        'elevs': 2D numpy array of elevations (meters)
        'lats': list of lats (inclusive bounds)
        'lons': list of lons (inclusive bounds)
    }
    '''
    mat = data_dict['mat']
    loni0 = find_nearest(data_dict['lons'], lon_lo)
    loni1 = find_nearest(data_dict['lons'], lon_hi)
    lati0 = find_nearest(data_dict['lats'], lat_lo)
    lati1 = find_nearest(data_dict['lats'], lat_hi)
    
    # lats grow up but matrices grow down, so invert
    latii1 = len(data_dict['lats']) - lati0  
    latii0 = len(data_dict['lats']) - lati1

    region = mat[latii0:latii1, loni0:loni1]

    d = {
        "elevs": region,
        "lats": data_dict['lats'][lati0:lati1],
        "lons": data_dict['lons'][loni0:loni1]
    }

    return d


def main():

    # Get a list of all the .HGT filenames in the directory
    hgt_files = list(glob(f'{HGT_FILES_DIR}/*.hgt'))
    model_boxes = getModelBoxes(local_filename=GPS_BOUNDS_FILE, remote=not bool(GPS_BOUNDS_FILE))

    region = getModelBoxRegion(model_boxes, region_name=MODEL_BOX_REGION_NAME)
    
    # Tile the hgt files into a 2D elevation matrix
    data = tileHGT(hgt_files)

    # Crop matrix by lat/lon bounds
    # and convert dictionary into a form that mimics AQ&U's
    mat_dict = extractRegion(
                data_dict=data,
                lat_lo=region['lat_lo'],
                lat_hi=region['lat_hi'],
                lon_lo=region['lon_lo'],
                lon_hi=region['lon_hi'])

    print('Lat Bounds:', [region['lat_lo'], region['lat_hi']])
    print('Lon Bounds:', [region['lon_lo'], region['lon_hi']])

    savemat(MAT_OUT, mat_dict)

    print(f'Saved to: {MAT_OUT}')
    

if __name__ == '__main__':
    main()
            
        