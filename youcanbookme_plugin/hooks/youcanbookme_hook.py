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
from requests.auth import HTTPBasicAuth

# Airflow Base Classes
from airflow.utils.log.logging_mixin import LoggingMixin

# Airflow Extended Classes
from airflow.hooks.base_hook import BaseHook


class YoucanbookmeHook(BaseHook, LoggingMixin):
    def __init__(
            self,
            conn_id,
            *args,
            **kwargs
    ):
        """
        Create new connection to Youcanbookme.
        :param conn_id: connection for Youcanbookme
        :type conn_id: string
        """
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson
        self.username = self.extras['username']
        self.password = self.extras['password']
        self.account_id = self.extras['account_id']

    def _get(self, path, params=None, auth={}, headers={}):
        response = requests.get(path,
                                 params=params,
                                 auth=auth,
                                 headers=headers)
        return json.loads(response.text)


    def retrieve_profiles(self, fields):
        auth = HTTPBasicAuth(self.username, self.password)
        base_url = 'https://api.youcanbook.me/v1/'
        
        params = {
        "fields": fields
        }

        response = self._get(base_url + self.account_id + '/profiles', params=params, auth=auth)
        return response

    def retrieve_booking(self, fields, profile_id, booking_id):
        auth = HTTPBasicAuth(self.username, self.password)
        base_url = 'https://api.youcanbook.me/v1/'
        
        params = {
        "fields": fields
        }

        path_extension = "/profiles/{profile_id}/bookings/{booking_id}".format(profile_id=str(profile_id), booking_id=str(booking_id))
        response = self._get(base_url + self.account_id + path_extension, params=params, auth=auth)
        return response
