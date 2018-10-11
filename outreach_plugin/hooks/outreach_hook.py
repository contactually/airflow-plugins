# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
This module contains a Salesforce Hook based off of the contrib version
which allows you to connect to your Salesforce instance,
retrieve data from it, and write data back to Salesforce.
NOTE:   this hook also relies on the simple_salesforce package:
        https://github.com/simple-salesforce/simple-salesforce
"""

# Widely Available Packages -- if it's a core Python package or on PyPi, put it here.
import json
import time
import requests
from urllib.parse import urlparse, parse_qs

# Airflow Base Classes
from airflow.utils.log.logging_mixin import LoggingMixin

# Airflow Extended Classes
from airflow.hooks.base_hook import BaseHook


class OutreachHook(BaseHook, LoggingMixin):
    """
    Interact with Outreach.io API.
    """

    def __init__(self,
                outreach_conn_id,
                *args,
                **kwargs
    ):
        """
        Create new connection to Outreach.io
        :param outreach_conn_id: name of connection to Outreach.io
        :type outreach_conn_id: str
        """
        self.outreach_conn_id = outreach_conn_id
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(outreach_conn_id)
        self.extras = self.connection.extra_dejson
        self.client_id = self.extras['client_id']
        self.client_secret = self.extras['client_secret']
        self.redirect_uri = self.extras['redirect_uri']

    def _get(self, path, params=None, auth={}, headers={}):
        response = requests.get(path,
                                 params=params,
                                 auth=auth,
                                 headers=headers)
        return json.loads(response.text)

    def _post(self, path, payload=None, auth={}, headers={}):
        response = requests.post(path,
                                 json=payload,
                                 auth=auth,
                                 headers=headers)
        return json.loads(response.text)

    def consume_refresh_token(self, refresh_token):
        oauth_refresh_path = 'https://api.outreach.io/oauth/token'

        payload = {
        "client_id": self.client_id,
        "client_secret": self.client_secret,
        "redirect_uri": self.redirect_uri,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
        }

        oauth_token = self._post(oauth_refresh_path, payload=payload)
        return oauth_token

    def retrieve_all(self, oauth_token, resource, filter=False, filter_field=None, filter_statement=None, page_limit=100, page_offset=0, sort=False, sort_statement=None):
        headers = {
        "Authorization": "Bearer {token}".format(token=oauth_token),
        "Accept": "application/vnd.api+json"
        }

        params = {
        "page[limit]": str(page_limit),
        "page[offset]": str(page_offset)
        }

        if filter:
            params["filter[{filter_field}]".format(filter_field=filter_field)] = filter_statement

        if sort:
            params["sort"] = sort_statement

        # loop through all pages and create payload
        base_url = 'https://api.outreach.io/api/v2/'
        offset = '0'
        next_offset = '0'
        continue_loop = True

        payload = []
        while continue_loop:
            response = self._get(base_url + resource, params=params, headers=headers)
            if not response['data']:
                break
            for record in response['data']:
                record_dict = {}
                record_dict['id'] = record['id']
                for key, value in record['attributes'].items():
                    record_dict[key] = value
                for key, value in record['relationships'].items():
                    nested_data = value.get('data')
                    if nested_data and isinstance(nested_data, list):
                        key = nested_data[0].get('type')
                        value = nested_data[0].get('id')
                        if key:
                            record_dict[key] = value
                        else:
                            continue
                    elif nested_data and isinstance(nested_data, dict):
                        key = nested_data.get('type')
                        value = nested_data.get('id')
                        if key:
                            record_dict[key] = value
                        else:
                            continue
                    else:
                        continue
                payload.append(record_dict)
            if response.get('links'):
                if response.get('links').get('next'):
                    full_url = response['links']['next']
                    output = urlparse(full_url)
                    qs = parse_qs(output.query)
                    next_offset = qs['page[offset]'][0]
                    params['page[offset]'] = next_offset
            if next_offset == offset:
                continue_loop = False
            else:
                offset = next_offset

        return payload