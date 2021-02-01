import os
import hashlib
import time
import configparser
import requests
import schedule
from utils import *


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
                 # leads_url: str,
                 # orders_url: str,
                 redash_api_key: str,
                 redash_leads_query_id: int,
                 redash_orders_query_id: int,
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
        # self.leads_url = leads_url
        # self.orders_url = orders_url
        self.redash_api_key = redash_api_key
        self.redash_leads_query_id = redash_leads_query_id
        self.redash_orders_query_id = redash_orders_query_id

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
                       self.fb_client_secret,
                       "&fb_exchange_token=",
                       self.fb_token))

        respond = requests.get(url)

        if respond.status_code != 200:
            raise Exception("Failed update Facebook access token "+response.text)

        self.fb_token = respond.json()['access_token']


    def upload_leads(self):
        """
        Query leads.
        """

        upload_facebook_data(self.redash_leads_query_id,
                             self.fb_customers_event,
                             self.redash_api_key,
                             self.fb_token)


    def upload_customers(self):

        upload_facebook_data(self.redash_orders_query_id,
                             self.fb_customers_event,
                             self.redash_api_key,
                             self.fb_token)


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
        schedule.every().day.at(t).do(self.upload_customers)

        while True:

            schedule.run_pending()
            time.sleep(1)

    def main(self, t):
        self.update_facebook_access_token()
        self.shedule_tasks(t)


if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.sections()
    config.read('./uploader.cfg')


    uploaderObject = FacebookUploader(fb_token=config['Facebook']['fb_token'],
                                      fb_client_id=config['Facebook']['fb_client_id'],
                                      fb_client_secret=config['Facebook']['fb_client_secret'],
                                      fb_lead_event=config['Facebook']['fb_lead_event'],
                                      fb_customers_event=config['Facebook']['fb_customers_event'],
                                      fb_api_ver = 'v9.0',
                                      redash_api_key=config['Redash']['api_key'],
                                      redash_orders_query_id=config['Redash']['orders_query_id'],
                                      redash_leads_query_id=config['Redash']['clients_query_id'])
    uploaderObject.main(config['Uploader']['time'])
