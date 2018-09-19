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
import zeep
import json
import time

# Airflow Base Classes
from airflow.utils.log.logging_mixin import LoggingMixin

# Airflow Extended Classes
from airflow.hooks.base_hook import BaseHook


class ZuoraSoapHook(BaseHook, LoggingMixin):
    def __init__(
            self,
            zuora_conn_id,
            wsdl_url='https://www.zuora.com/apps/services/a/91.0',
            *args,
            **kwargs
    ):
        """
        Create new connection to Zuora
        and allows you to pull data out of Zuora.
        :param zuora_conn_id:     the name of the connection that has the parameters
                            we need to connect to Zuora.
                            The connection shoud be type `http` and include the API endpoint in the `Extras` field.
        .. note::
            For the HTTP connection type, you can include a
            JSON structure in the `Extras` field.
            We need the appropriate API endpoint for production vs sandbox.
            So we define it in the `Extras` field as:
                `{"endpoint":"ENDPOINT"}`
        """
        self.zuora_conn_id = zuora_conn_id
        self.wsdl_url = wsdl_url
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(zuora_conn_id)
        self.extras = self.connection.extra_dejson

    def sign_in(self):
        """
        Sign into Salesforce.
        If we have already signed in, this will just return the original object
        """
        if hasattr(self, 'client'):
            return self.client

        # connect to Salesforce
        client = zeep.Client(wsdl=self.wsdl_url)
        response = client.service.login(
            username=self.connection.login,
            password=self.connection.password)

        self.session_id = response['Session']
        self.client = client

        return client

    def query(self, query):
        """
        Queries Zuora and returns all records.
        :param query: query string
        :type query: string
        """

        self.sign_in()

        records = []
        query = query.replace('<', '&lt;')

        response = self.client.service.query(query, _soapheaders={'SessionHeader': self.session_id})
        records += response['records']

        while not response['done']:
            response = self.client.service.query_more(response['queryLocator'])
            records += response['records']

        return records