import os
import math 
import numpy as np
from scipy.io import savemat
from scipy.ndimage.measurements import label
from matplotlib import pyplot as plt
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 unused import
from mpl_toolkits.mplot3d.axes3d import Axes3D
from mpl_toolkits.mplot3d import proj3d
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.ticker import LinearLocator, FormatStrFormatter
import numpy as np
from glob import glob
import json 

def find_nearest(array, value):
    array = np.asarray(array)
    return (np.abs(array - value)).argmin()


def hgt2Mat(hgt_filename):
    siz = os.path.getsize(hgt_filename)
    dim = int(math.sqrt(siz / 2))
    assert dim * dim * 2 == siz, "Invalid file size"
    return np.fromfile(hgt_filename, np.dtype('>i2'), dim * dim).reshape((dim, dim))


def tileHGT(hgt_list):
    SZ = 3600
    lat_lo, lon_lo =  1000,  1000
    lat_hi, lon_hi = -1000, -1000
    counter = {}
    cnt = 0
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

        counter[cnt] = {
            'filename': fn,
            'lat': lat,
            'lon': lon,
            'mat': hgt2Mat(fn)
        }

        cnt += 1

    szX = (((lon_hi + 1) - lon_lo) * SZ) + 1
    szY = (((lat_hi + 1) - lat_lo) * SZ) + 1
    canvas = np.zeros((szY, szX))

    for k, v in counter.items():
        x0 = (v['lon'] - lon_lo) * SZ
        y0 = (v['lat'] - lat_lo) * SZ
        canvas[y0:y0+SZ+1, x0:x0+SZ+1] = v['mat']
    

    lon_locs = np.linspace(lon_lo, lon_hi + 1, szX)
    lat_locs = np.linspace(lat_lo, lat_hi + 1, szY)

    return {
        "lats": lat_locs,
        "lons": lon_locs,
        "mat": canvas
    }


def extractRegion(data_dict, lat_lo, lat_hi, lon_lo, lon_hi):
    mat = data_dict['mat']
    loni0 = find_nearest(data_dict['lons'], lon_lo)
    loni1 = find_nearest(data_dict['lons'], lon_hi)
    lati0 = find_nearest(data_dict['lats'], lat_lo)
    lati1 = find_nearest(data_dict['lats'], lat_hi)
    
    # lats grow up but matrices grow down, so invert
    lati0 = len(data_dict['lats']) - lati0  
    lati1 = len(data_dict['lats']) - lati1
    lati0, lati1 = lati1, lati0

    region = mat[lati0:lati1, loni0:loni1]

    return region


def main():
    
    DATA_DIR = 'chatt'
    MAT_OUT = f'/Users/tombo/uu/TBECNEL/Tetrad/tetrad_site/tools/LPDAAC/{DATA_DIR}.mat'
    
    
    files = list(glob(f'/Users/tombo/uu/TBECNEL/Tetrad/tetrad_site/tools/LPDAAC/{DATA_DIR}/*.hgt'))
    model_boxes = json.load(open('/Users/tombo/uu/TBECNEL/Tetrad/tetrad_site/gcp-tasks/model_boxes.json'))
    for region in model_boxes:
        if region['table'] == 'slc_ut': 
            break

    data = tileHGT(files)
    elevs = extractRegion(
                data_dict=data,
                lat_lo=region['lat_lo'],
                lat_hi=region['lat_hi'],
                lon_lo=region['lon_lo'],
                lon_hi=region['lon_hi'])
    
    final = {
        "lats":  data["lats"],
        "lons":  data["lons"],
        "elevs": elevs
    }

    savemat(MAT_OUT, final)

    print('Saved...')
    

if __name__ == '__main__':
    main()
    # structure = np.ones((3,3))
    # labeled, nc = label(mask, structure)
    # assert nc == 1, "All file boxes must be touching"

    # indices = np.indices(mask.shape)
    # indices = np.moveaxis(indices, 0, -1)
    # indices[labeled==1]
            
        