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
from zuora_restful_python.zuora import Zuora
from io import StringIO
import json
import time
import csv

# Airflow Base Classes
from airflow.utils.log.logging_mixin import LoggingMixin

# Airflow Extended Classes
from airflow.hooks.base_hook import BaseHook


class ZuoraRestHook(BaseHook, LoggingMixin):
    def __init__(
            self,
            zuora_conn_id,
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
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(zuora_conn_id)
        self.extras = self.connection.extra_dejson

    def sign_in(self):
        """
        Sign into Zuora.
        If we have already signed it, this will just return the original object
        """
        if hasattr(self, 'z'):
            return self.z

        z = Zuora(
            username=self.connection.login,
            password=self.connection.password,
            endpoint=self.extras.get('endpoint', 'production')
        )
        self.z = z
        return z

    def query(self, query):
        """
        Queries Zuora and returns all records.
        :param query: query string
        :type query: string
        """

        self.sign_in()

        result = self.z.query_all(query)
        return result

    def update_object(self, object_name, object_id, payload):
        """
        Updates existing Zuora object.
        :param object_name: name of object to update
        :type query: string
        :param object_id: ID of object to update
        :type object_id: string
        :param payload: REST payload for update
        :type payload: dictionary or list of dictionaries
        """

        self.sign_in()

        result = self.z.update_object(object_name, object_id, payload)
        return result

    def create_object(self, object_name, payload):
        """
        Creates new Zuora object.
        :param object_name: name of object to create
        :type query: string
        :param payload: REST payload for creation
        :type payload: dictionary or list of dictionaries
        """

        self.sign_in()

        result = self.z.create_object(object_name, payload)
        return result

    def create_bill_run(self, invoice_date, target_date,
                        account_id=None,
                        auto_email=False,
                        auto_post=False,
                        auto_renewal=False,
                        batch='AllBatches',
                        bill_cycle_day='AllBillCycleDays',
                        charge_type_to_exclude='',
                        no_email_for_zero_amount_invoice=False):
        """
        Creates a new bill run.
        :param invoice_date: date of invoice
        :type invoice_date: string or datetime.datetime
        :param target_date: target date of bill run
        :type target_date: string or datetime.datetime
        :param account_id: account_id related to bill run
        :type account_id: string
        :param auto_email: tells whether to email bill run recipients automatically
        :type auto_email: boolean
        :param auto_post: automatically post bill run
        :type auto_post: boolean
        :param auto_renewal: automatically renew terminated subscriptions
        :type auto_renewal: boolean
        :param batch: batch ID to create bill run for
        :type batch: string
        :param charge_type_to_exclude: charge type to exclude from bill run
        :type charge_type_to_exclude: string
        :param no_email_for_zero_amount_invoice: whether or not to email zero invoice amounts
        :type no_email_for_zero_amount_invoice: boolean
        """

        self.sign_in()

        result = self.z.create_bill_run(invoice_date, target_date, account_id, auto_email, auto_post, auto_renewal, batch, bill_cycle_day, charge_type_to_exclude, no_email_for_zero_amount_invoice)
        return result

    def create_credit_balance_adjustment(self, payload):
        """
        Creates a credit balance adjustment.
        :param payload: REST payload
        :type payload: dictionary
        """

        self.sign_in()

        result = self.z.create_credit_balance_adjustment(payload)
        return result

    def cancel_subscription(self, subscription_id, payload):
        """
        Creates a credit balance adjustment.
        :param subscription_id: subscription ID for cancellation
        :type subscription_id: string
        :param payload: REST payload
        :type payload: dictionary
        """

        self.sign_in()

        result = self.z.cancel_subscription(subscription_id, payload)
        return result

    def query_export(self, query):
        """
        Creates and runs a query export.
        :param query: ZOQL query
        :type query: string
        """

        self.sign_in()

        response = self.z.query_export(query)
        return response

    def aqua_query(self, query):
        """
        Creates and runs a query via the AQuA API.
        :param query: ZOQL query
        :type query: string
        """

        self.sign_in()

        query_param = [{"name": "ZOQLQUERY",
                        "query": query,
                        "type": "zoqlexport"}]

        payload = {"format": "csv",
                   "name": "Example",
                   "encrypted": "none",
                   "useQueryLabels": "true",
                   "dateTimeUtc": "true",
                   "queries": query_param}

        response = self.z._post("/batch-query/", payload)
        response_status = response['status']

        error_statuses = ['aborted', 'cancelled', 'error']
        running_statuses = ['submitted', 'executing', 'pending']

        status = response_status
        if status in error_statuses:
            return self.log.error("Error: {error_message} Query: {query}".format(error_message=response['message'], query=response['batches'][0]['query']))

        check_job_path = "/batch-query/jobs/{id}".format(id=response['id'])

        while status in running_statuses:
            time.sleep(5)
            check_response = self.z._get(check_job_path)
            status = check_response['status']

        file_id = check_response['batches'][0]['fileId']

        results = self.z.get_files(file_id)
        results = StringIO(results)
        results = csv.DictReader(results, delimiter=",")

        return results
