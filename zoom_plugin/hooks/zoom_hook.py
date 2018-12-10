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

# Widely Available Packages -- if it's a core Python package or on PyPi, put it here.
import json
import time
import requests
import jwt
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta

# Airflow Base Classes
from airflow.utils.log.logging_mixin import LoggingMixin

# Airflow Extended Classes
from airflow.hooks.base_hook import BaseHook


class ZoomHook(BaseHook, LoggingMixin):
    """
    Interact with Zoom.us API.
    """

    def __init__(self,
                zoom_conn_id,
                *args,
                **kwargs
    ):
        """
        Create new connection to Zoom.us
        :param outreach_conn_id: name of connection to Zoom.us
        :type outreach_conn_id: str
        """
        self.zoom_conn_id = zoom_conn_id
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(zoom_conn_id)
        self.extras = self.connection.extra_dejson
        self.api_key = self.extras['api_key']
        self.secret = self.extras['secret']
        self.base_path = 'https://api.zoom.us/v2/'

        payload = {'iss': self.api_key, 'exp': datetime.utcnow() + timedelta(seconds=5000)}

        self.token = jwt.encode(payload, self.secret, algorithm='HS256')

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

    def list_users(self):
        path = self.base_path + '/users'
        current_page = 1
        paginate = True

        results = []
        while paginate:
            response = self._get(path, params={'access_token': self.token, 'page_size': 300, 'page_number': current_page})
            results.extend(response.get('users', []))
            page_count = response.get('page_count')
            paginate = False if current_page == page_count or page_count == 0 else True
            current_page += 1

        return results

    def list_webinars(self, user_id):
        path = self.base_path + '/users/{user_id}/webinars'.format(user_id=user_id)
        current_page = 1
        paginate = True

        results = []
        while paginate:
            response = self._get(path, params={'access_token': self.token, 'page_size': 300, 'page_number': current_page})
            results.extend(response.get('webinars', []))
            page_count = response.get('page_count')
            paginate = False if current_page == page_count or page_count == 0 else True
            current_page += 1

        return results

    def list_registrants(self, webinar_id):
        path = self.base_path + '/webinars/{webinar_id}/registrants'.format(webinar_id=webinar_id)
        current_page = 1
        paginate = True

        results = []
        while paginate:
            response = self._get(path, params={'access_token': self.token, 'page_size': 300, 'page_number': current_page})
            results.extend(response.get('registrants', []))
            page_count = response.get('page_count')
            paginate = False if current_page == page_count or page_count == 0 else True
            current_page += 1

        return results

    def list_participants(self, webinar_id):
        path = self.base_path + '/report/webinars/{webinar_id}/participants'.format(webinar_id=webinar_id)
        current_page = 1
        paginate = True

        results=[]
        while paginate:
            response = self._get(path, params={'access_token': self.token, 'page_size': 300, 'page_number': current_page})
            results.extend(response.get('participants', []))
            page_count = response.get('page_count')
            paginate = False if current_page == page_count or page_count == 0 or response.get('code') in [403, 3001] else True
            current_page += 1

        return results

    def retrieve_webinar(self, webinar_id):
        path = self.base_path + '/webinars/{webinar_id}'.format(webinar_id=webinar_id)
        response = self._get(path, params={'access_token': self.token})
        key_list = ['uuid', 'id', 'host_id', 'topic', 'type', 'start_time', 'duration', 'timezone', 'created_at', 'join_url']
        results = [{k: v for k, v in response.items() if k in key_list}]

        return results
