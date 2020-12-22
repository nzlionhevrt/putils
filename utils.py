import os
import requests
import time
import pandas as pd
from datetime import datetime


def poll_job(s, redash_url, job):
    while job['status'] not in (3,4):
        response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
        job = response.json()['job']
        time.sleep(1)

    if job['status'] == 3:
        return job['query_result_id']
    
    return None


def get_fresh_query_result(redash_url, query_id, api_key, params):
    s = requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})

    response = s.post('{}/api/queries/{}/refresh'.format(redash_url, query_id), params=params)

    if response.status_code != 200:
        print(response.text)
        raise Exception('Refresh failed.')

    result_id = poll_job(s, redash_url, response.json()['job'])

    if result_id:
        response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))
        if response.status_code != 200:
            raise Exception('Failed getting results.')
    else:
        raise Exception('Query execution failed.')

    return response.json()['query_result']['data']['rows']


def to_table(json_array):
    
    data = {}

    for row in json_array:
        for key in row:
            if key in data:
                data[key].append(row[key])
            else:
                data[key] = [row[key]]

    return pd.DataFrame(data)


def proccess(params, query_id):

    output = get_fresh_query_result('https://redash.base.pedant.ru', query_id, api_key, params)
    if not output:
        return None
        
    data = to_table(output)
    return data
    

def fb_upload_data(event_id, 
		   params, 
		   json, 
		   api_ver='v9.0'):

    s = requests.Session()
    
    url = f'https://graph.facebook.com/{api_ver}/{event_id}/events'
    response = s.post(url, params=params)

    if response.status_code != 200:
        print(response.text)
        raise Exception('Refresh failed.')

    return response.json()


def fb_upload_data(event_id, 
		   params, 
		   json, 
		   api_ver='v9.0'):

    s = requests.Session()
    
    url = f'https://graph.facebook.com/{api_ver}/{event_id}/events'
    response = s.post(url, params=params, json=json)

    if response.status_code != 200:
        print(response.text)
        raise Exception('Refresh failed.')

    return response.json()



def fb_create_event(params, 
		    json, 
		    api_ver='v9.0'):

    s = requests.Session()
    
    url = f'https://graph.facebook.com/{api_ver}/{manager_id}/offline_conversion_data_sets'
    response = s.post(url, params=params)

    if response.status_code != 200:
        print(response.text)
        raise Exception('Refresh failed.')

    return response.json()


