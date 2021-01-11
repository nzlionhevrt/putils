import os
import hashlib
import time
import configparser
import requests
import schedule


class MetaSingleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class FacebookUploader(metaclass=MetaSingleton):

    def __init__(self,
                 fb_token: str, fb_client_id: str,
                 fb_client_secret: str, fb_lead_event: str,
                 fb_customers_event: str,
                 leads_url: str,
                 orders_url: str,
                 fb_api_ver = 'v9.0'
                 ) -> None:

        """
        Init class object.
        """

        self.connection = None

        self.fb_token = fb_token
        self.fb_client_id = fb_client_id
        self.fb_client_secret = fb_client_secret
        self.fb_api_ver = fb_api_ver
        self.fb_lead_event = fb_lead_event
        self.fb_customers_event = fb_customers_event
        self.leads_url = leads_url
        self.orders_url = orders_url
        self.update_facebook_access_token()


    def get_data(self, url: str):
        """
        Givin a url makes get request and returns a data.
        """

        respond = requests.get(url)

        if response.status_code != 200:
            raise Exception("Failed load data from "+url)

        return respond.json()

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
        """
        Query leads.
        """

        data = self.get_data(self.leads_url)

        upload_json = []

        for row in data:
            upload_json.append({
                "event_time" : int(row['time']),
                "match_keys" : {
                    "phone" : [hashlib.sha256(row['phone'].encode('utf-8')).hexdigest()]
                    },
                "event_name" : "Lead"
            })

        del data


        # upload data via Facebook api
        for i in range(0, 200, step):

            params = {
                'access_token' : self.fb_token,
                'upload_tag' : 'stored_data',
                'data' : json.dumps(upload_json[i:i+step])
                }

            self.fb_upload_data(event_id, params=params, json=None)

        del upload_json


    def upload_customers(self):

        data = self.get_data(self.orders_url)

        upload_json = []

        for row in data:
            upload_json.append({
                "event_time" : int(row['time']),
                "match_keys" : {
                    "phone" : [hashlib.sha256(row['phone'].encode('utf-8')).hexdigest()]
                    },
                "event_name" : "Lead",
                "value" : int(row['price']),
                "currency" : 'USD'
            })

        del data

        #upload data via facebook api
        for i in range(0, 200, step):

            params = {
                'access_token' : self.fb_token,
                'upload_tag' : 'stored_data',
                'data' : json.dumps(upload_json[i:i+step])
                }

            self.fb_upload_data(event_id, params=params, json=None)

        del upload_json


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


    def shedule_tasks(self, t):
        """
        Launch shedule execution for methods.
        """
        schedule.every().day.at(t).do(self.update_facebook_access_token)
        schedule.every().day.at(t).do(self.upload_leads)
        # schedule.every().day.at(t).do(self.upload_customers)

        while True:

            schedule.run_pending()
            time.sleep(1)

    def main(t):
        self.update_facebook_access_token()
        self.shedule_tasks(t)


if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.sections()
    config.read('./uploader.cfg')

    if 'dbpassword' not in config['Database']:
        password = None
    else:
        password = config['Database']['dbpassword']

    uploaderObject = FacebookUploader(fb_token=config['Facebook']['fb_token'],
                                      fb_client_id=config['Facebook']['fb_client_id'],
                                      fb_client_secret=config['Facebook']['fb_client_secret'],
                                      fb_lead_event=config['Facebook']['fb_lead_event'],
                                      fb_customers_event=config['Facebook']['fb_customers_event'],
                                      fb_api_ver = 'v9.0')
    uploaderObject.main()
