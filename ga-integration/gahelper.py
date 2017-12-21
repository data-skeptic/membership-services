import pandas as pd
import requests
import os
import argparse

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

from requests_oauthlib import OAuth1

class Gahelper(object):
    
    def get_service(self, api_name, api_version, scope, key_file_location, service_account_email):
      credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location, scopes=scope)
      http = credentials.authorize(httplib2.Http())
      service = build(api_name, api_version, http=http)
      return service


    def get_first_profile_id(self, service):
      accounts = service.management().accounts().list().execute()
      if accounts.get('items'):
        account = accounts.get('items')[0].get('id')
        properties = service.management().webproperties().list(accountId=account).execute()
        if properties.get('items'):
          property = properties.get('items')[0].get('id')
          profiles = service.management().profiles().list(accountId=account, webPropertyId=property).execute()
          if profiles.get('items'):
            return profiles.get('items')[0].get('id')
      return None


    def get_results(self, service, profile_id):
      return service.data().ga().get(
          ids='ga:' + profile_id,
          start_date='7daysAgo',
          end_date='today',
          metrics='ga:sessions').execute()


    def print_results(self, results):
      if results:
        print('View (Profile): %s' % results.get('profileInfo').get('profileName'))
        print('Total Sessions: %s' % results.get('rows')[0][0])
      else:
        print('No results found')
  
   
    def initialize(self, scope, key_file_location, service_account_email):
        self.service = self.get_service('analytics', 'v3', scope, key_file_location, service_account_email)
        self.service.management().accounts().list().execute()
        accounts = self.service.management().accounts().list().execute()
        items = accounts.get('items')
        account = items[0].get('id')
        properties = self.service.management().webproperties().list(accountId=account).execute()
        items2 = properties.get('items')
        property = items2[0].get('id')
        profiles = self.service.management().profiles().list(accountId=account, webPropertyId=property).execute()
        items3 = profiles.get('items')
        self.profile = self.get_first_profile_id(self.service)
        self.print_results(self.get_results(self.service, self.profile))
 
 
    def get_report(self, metrics, dimensions, start_date, end_date):
        metric = ','.join(metrics)
        dimension = ','.join(dimensions)
        if len(dimension) > 0:
          try:
            resp = self.service.data().ga().get(
                  ids='ga:' + self.profile,
                  start_date=start_date,
                  end_date=end_date,
                  metrics=metric,
                  dimensions=dimension
            ).execute()
          except:
            print('error in get_report')
            pass
        else:
          print("There is no dimension.")
          try:
            resp = self.service.data().ga().get(
                  ids='ga:' + self.profile,
                  start_date=start_date,
                  end_date=end_date,
                  metrics=metric
            ).execute()
          except:
            print('error in get_report')
            pass
        df = pd.DataFrame(resp.get('rows'))
        cols = []
        for d in dimensions:
            cols.append(d.replace('ga:', ''))
        for m in metrics:
            cols.append(m.replace('ga:', ''))
        df.columns = cols
        return df


    def __init__(self, config):
        self.scope = ['https://www.googleapis.com/auth/analytics.readonly']
        self.service_account_email = config["service_account_email"]
        self.key_file_location = config['key_file_location']
        self.dir_path = os.path.dirname(os.path.realpath(__file__))
        self.key_file_location = "/".join(self.dir_path.split("/")[0:-1]) + self.key_file_location
        self.initialize(self.scope, self.key_file_location, self.service_account_email)
        
