#!/usr/bin/env python3
"""
This is a script put together to pull Impala query statistics and generate a
report to help guide the deployment of Impala Admission Control.
"""
#impala-qmonitor querymonitor 

import argparse
from configparser import ConfigParser
import json
import datetime
import copy
import os.path
import numpy as np
import requests
import urllib3
urllib3.disable_warnings()




def query_running_from_more_than_1hr(cloudera_api, query_duration):


    # api_url = '%s://%s:%s/api/%s/clusters/%s/services/impala/impalaQueries?filter=query_duration%3E%s&queryState=RUNNING' % \
    #         (cloudera_api['protocol'],
    #          cloudera_api['hostname'],
    #          cloudera_api['port'],
    #          cloudera_api['version'],
    #          cloudera_api['cluster'],
    #          query_duration)    

    api_url = '%s://%s:%s/api/%s/clusters/%s/services/impala/impalaQueries' % \
            (cloudera_api['protocol'],
             cloudera_api['hostname'],
             cloudera_api['port'],
             cloudera_api['version'],
             cloudera_api['cluster']) 

    response = requests.get(tmp_url, auth=(cloudera_api['username'], cloudera_api['password']), verify=False)
    if response.status_code != 200:
            exit(response.text)

    for query in json_data['queries']:
      print query
      

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", action="store", dest="config",
                        required=True, help="path to your configuration file")
    # parser.add_argument("-d", "--days", action="store", dest="number_of_days",
    #                     required=True, help="number of days to query")
    args = parser.parse_args()

    # Does the config file actually exist?
    if os.path.exists(args.config) is False:
        exit('invalid config file')

    # Create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(args.config)

    # Get config section
    cloudera_api = {}
    items = parser.items('config')
    for item in items:
        cloudera_api[item[0]] = item[1]

    query_running_from_more_than_1hr(cloudera_api, 1):

    # main(cloudera_api, int(args.number_of_days))

