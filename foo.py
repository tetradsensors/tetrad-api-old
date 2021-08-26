import requests 
from common.utils import update_dict_recursive

d = {
    'routea': {
        'limit': 1,
        'hoursToRecharge': 1
    }
}

update = {'routea': {
    'limit': None
}, 'username': 1}

print(update_dict_recursive(d, update))


