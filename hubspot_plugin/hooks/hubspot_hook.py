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
from collections import OrderedDict

# Airflow Base Classes
from airflow.utils.log.logging_mixin import LoggingMixin

# Airflow Extended Classes
from airflow.hooks.base_hook import BaseHook


class HubspotHook(BaseHook, LoggingMixin):
    def __init__(
            self,
            conn_id,
            *args,
            **kwargs
    ):
        """
        Create new connection to Hubspot
        """
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson
        self.api_key = self.extras['api_key']
        self.base_url = 'https://api.hubapi.com/contacts/v1/'

    def _get(self, path, params=None, auth={}, headers={}):
        response = requests.get(path,
                                 params=params,
                                 auth=auth,
                                 headers=headers)
        return response

    def _post(self, path, params=None, payload=None, auth={}, headers={}):
        response = requests.post(path,
                                 params=params,
                                 json=payload,
                                 auth=auth,
                                 headers=headers)
        return response

    def _delete(self, path, params=None):
        params = {'hapikey': self.api_key}
        response = requests.delete(path, 
                                   params=params)
        return response

    def delete_contacts(self, payload):
        for record in payload:
            contact_id = record['contact_id']
            path = self.base_url + 'contact/vid/{contact_id}'.format(contact_id=contact_id)
            response = self._delete(path)
            if not response.get('deleted'):
                self.log.error("Contact failed to delete with status {status_code} and error {message}".format(status_code=response.status_code, message=response.text))

        return self.log.info("Delete contacts completed!")

    def upsert_contacts(self, payload):
        hubspot_payload = []
        for record in payload:
            email = record.pop('email')
            record_dict = {"email": email, "properties": [{"property": k, "value": v} for k,v in record.items()]}
            hubspot_payload.append(record_dict)

        batchsize = 100
        params = {'hapikey': self.api_key}

        self.log.info("Batch upserting {payload_size} contacts...".format(payload_size=len(hubspot_payload)))
        for i in range(0, len(hubspot_payload), batchsize):
            batch = hubspot_payload[i:i+batchsize]
            path = self.base_url + 'contact/batch/'
            self.log.info("Posting contacts {begin_batch} thru {end_batch}".format(begin_batch=i+1, end_batch=i+100 if i+100 < len(hubspot_payload) else len(hubspot_payload)))
            response = self._post(path, params=params, payload=batch)

            if response.status_code != 202:
                self.log.info("Batch upsert failed, switching to individual upsert...")
                for record in batch:
                    path = self.base_url + "contact/createOrUpdate/email/{email}".format(email=record['email'])
                    response = self._post(path, params=params, payload=batch)
                    if response.status_code == 200:
                        self.log.info("Upserted {record}".format(record=record))
                    else:
                        self.log.error("Upsert failed with status {status_code} and error {message}".format(status_code=response.status_code, message=response.text))

        return self.log.info("Upsert contacts completed!")