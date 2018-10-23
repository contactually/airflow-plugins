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
import base64

# Airflow Base Classes
from airflow.utils.log.logging_mixin import LoggingMixin

# Airflow Extended Classes
from airflow.hooks.base_hook import BaseHook


class GoToWebinarHook(BaseHook, LoggingMixin):
    """
    Interact with Outreach.io API.
    """

    def __init__(self,
                conn_id,
                *args,
                **kwargs
    ):
        """
        Create new connection to Outreach.io
        :param outreach_conn_id: name of connection to Outreach.io
        :type outreach_conn_id: str
        """
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson
        self.org_key = self.extras['org_key']
        self.consumer_key = self.extras['consumer_key']
        self.consumer_secret = self.extras['consumer_secret']
        self.base_url = "https://api.getgo.com/G2W/rest/v2/"

    def _get(self, path, params=None, auth={}, headers={}):
        response = requests.get(path,
                                 params=params,
                                 auth=auth,
                                 headers=headers)
        return json.loads(response.text)

    def _post(self, path, payload=None, auth={}, headers={}):
        response = requests.post(path,
                                 data=payload,
                                 auth=auth,
                                 headers=headers)
        return json.loads(response.text)

    def consume_refresh_token(self, refresh_token):
        oauth_refresh_path = 'https://api.getgo.com/oauth/v2/token'

        auth = str.encode("{consumer_key}:{consumer_secret}".format(consumer_key=self.consumer_key, consumer_secret=self.consumer_secret))

        b64_auth = base64.b64encode(auth)
        auth_header = b64_auth.decode('ascii')

        headers = {"Authorization": "Basic {auth}".format(auth=auth_header),
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
        }

        payload = {
          'grant_type': 'refresh_token',
          'refresh_token': refresh_token
        }

        response = self._post(oauth_refresh_path, headers=headers, payload=payload)
        return response

    def get_webinars(self, access_token, organizer_key, from_time, to_time):
        url = self.base_url + "organizers/{organizer_key}/webinars".format(organizer_key=organizer_key)

        headers = {"Authorization": "{access_token}".format(access_token=access_token),
        "Accept": "application/json"
        }

        params = {"fromTime": from_time,
        "toTime": to_time}

        response = self._get(url, params=params, headers=headers)
        return response

    def get_sessions(self, access_token, organizer_key, webinar_key):
        url = self.base_url + "organizers/{organizer_key}/webinars/{webinar_key}/sessions".format(organizer_key=organizer_key, webinar_key=webinar_key)


        headers = {"Authorization": "{access_token}".format(access_token=access_token),
        "Accept": "application/json"
        }

        response = self._get(url, headers=headers)
        return response

    def get_registrants(self, access_token, organizer_key, webinar_key):
        url = self.base_url + "organizers/{organizer_key}/webinars/{webinar_key}/registrants".format(organizer_key=organizer_key, webinar_key=webinar_key)


        headers = {"Authorization": "{access_token}".format(access_token=access_token),
        "Accept": "application/json"
        }

        response = self._get(url, headers=headers)
        return response

    def get_attendees(self, access_token, organizer_key, webinar_key, session_key):
        url = self.base_url + "organizers/{organizer_key}/webinars/{webinar_key}/sessions/{session_key}/attendees".format(organizer_key=organizer_key, webinar_key=webinar_key, session_key=session_key)


        headers = {"Authorization": "{access_token}".format(access_token=access_token),
        "Accept": "application/json"
        }

        response = self._get(url, headers=headers)
        return response



