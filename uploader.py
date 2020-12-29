import os
import hashlib
import time
import requests
import psycopg2
import shedule

import sql


class MetaSingleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]



class FacebookUploader(metaclass=MetaSingleton):

    def __init__(self, dbhost: str, dbport: str,
                 dbuser: str, dbname: str, dbpassword: str,
                 fb_token: str, fb_client_id: str,
                 fb_client_secret: str, fb_api_ver = 'v9.0,
                 ) -> None:
        """
        Giving a set of parameters, e.g. host, port,
        database name, username user password initialize
        connection to Posgress Database
        """

        self.init_conection(database=dbname, user=dbuser,
                            password=dbpassword,
                            host=dbhost, port=dbport)

        self.fb_token = fb_token
        self.fb_client_id = fb_client_id
        self.fb_client_secret = fb_client_secret
        self.fb_api_ver = fb_api_ver
        self.update_facebook_access_token()


def init_conection(self, dbhost: str, dbport: str,
                   dbuser: str, dbname: str, dbpassword: str):

    self.connection = psycopg2.connect(database=dbname, user=dbuser,
                                       password=dbpassword,
                                       host=dbhost, port=dbport)

    assert self.connection.status == psycopg2.extensions.STATUS_READY,\
           "Error connection to database..."



    def exec_query(self, query: str):
        """
        Giving a sql query string executes query and
        returns all rows.
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        return data


    def update_facebook_access_token(self):
        """
        Updates facebook token
        """
        url = ''.join(("https://graph.facebook.com/",
                       self.fb_api_ver,
                       "/oauth/access_token?grant_type=fb_exchange_token&client_id=",
                       self.fb_client_id,
                       "&client_secret=",
                       self.client_secret,
                       "&fb_exchange_token=",
                       self.fb_token))

        respond = requests.get(url)

        if response.status_code != 200:
            raise Exception("Failed update Facebook access token "+response.text)

        self.fb_token = respond.json()['access_token']


    def upload_leads(self):
        query = sql.query_leads_from_calls()
        data = self.exec_query(query)

        upload_json = []

        for row in data:
            upload_json.append({
                "event_time" : int(row[0]),
                "match_keys" : {"phone" : [hashlib.sha256(row[1].encode('utf-8')).hexdigest()]},
                "event_name" : "Lead"
            })

        del data

        for i in range(0, 200, step):

            params = {
                'access_token' : self.fb_token,
                'upload_tag' : 'stored_data',
                'data' : json.dumps(upload_json[i:i+step])
                }

            self.fb_upload_data(event_id, params=params, json=None)


    def upload_customers(self):
        query = sql.query_customers()
        data = self.exec_query(query)

        upload_json = []

        for row in data:
            upload_json.append({
                "event_time" : int(row[0]),
                "match_keys" : {"phone" : [hashlib.sha256(row[1].encode('utf-8')).hexdigest()]},
                "event_name" : "Lead",
                "value" : int(row[2]),
                "currency" : 'USD'
            })

        del data

        for i in range(0, 200, step):

            params = {
                'access_token' : self.fb_token,
                'upload_tag' : 'stored_data',
                'data' : json.dumps(upload_json[i:i+step])
                }

            self.fb_upload_data(event_id, params=params, json=None)


    def fb_upload_data(self,
                       event_id,
                       params,
                       json,
                       api_ver='v9.0'):

        s = requests.Session()

        url = f'https://graph.facebook.com/{api_ver}/{event_id}/events'
        response = s.post(url, params=params)

        if response.status_code != 200:
            print(response.text)
            raise Exception('Refresh failed.')

        return response.json(), response.status_code


    def shedule_tasks(self, time):
        schedule.every().day.at(time).do(self.upload_leads)
        schedule.every().day.at(time).do(self.upload_customers)

        while True:

            schedule.run_pending()
            time.sleep(1)


    def main():
        
