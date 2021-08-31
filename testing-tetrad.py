import urllib.request
import json
import itertools
import csv
import pandas as pd
import ssl
import numpy as np
import datetime 
myssl = ssl.create_default_context();
myssl.check_hostname=False
myssl.verify_mode=ssl.CERT_NONE

class bc:
    HEADER    = '\033[95m'
    BLUE      = '\033[94m'
    CYAN      = '\033[96m'
    GREEN     = '\033[92m'
    ORANGE    = '\033[93m'
    RED       = '\033[91m'
    ENDC      = '\033[0m'
    BOLD      = '\033[1m'
    UNDERLINE = '\033[4m'
    def o(s):
        return bc.ORANGE + s + bc.ENDC
    def r(s):
        return bc.RED + s + bc.ENDC
    def g(s):
        return bc.GREEN + s + bc.ENDC 
    def b(s):
        return bc.BLUE + s + bc.ENDC 
    def c(s):
        return bc.CYAN + s + bc.ENDC

sources = ['Tetrad', 'AQ%26U', 'PurpleAir', 'all']
regions = ['slc_ut', 'chatt_tn', 'clev_oh', 'kc_mo', 'pv_ma', 'all']

remote_server = "https://tetrad-api-qnofmwqtgq-uc.a.run.app/api"
local_server = "https://127.0.0.1:8080/api"

server = local_server


def executeQueries(query_list, head=10, print_status=False):
    for q in query_list:

        # If it's a tuple the 2nd param is whether to print some of the result
        if isinstance(q, tuple):
            q, status = q
        else:
            status = print_status

        print(q)
        req = urllib.request.Request(q)
        try:
            r = urllib.request.urlopen(req, context=myssl)
        except urllib.error.URLError as e:
            print(e.reason)
        else:
            json_data = json.loads(r.read())
            objstr = json.dumps(json_data, sort_keys=True, indent=4)
            if not isinstance(json_data, list): 
                print(bc.r('ERROR 1:'), objstr)
            elif isinstance(json_data, list) and len(json_data) == 0:
                print(bc.o('EMPTY'))
            elif isinstance(json_data, list):
                print(bc.g('OK'), len(json_data))
            else:
                print(bc.r('ERROR 2:'), objstr)
            if status:
                print('\n'.join(objstr.split('\n')[:head]))


query_list = [
f'{server}/'
f'{server}/'
f'{server}/'
f'{server}/'
f'{server}/'
f'{server}/'
f'{server}/'
]

executeQueries(query_list, head=25, print_status=True)
