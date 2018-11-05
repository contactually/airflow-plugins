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

# Widely Available Packages -- if it's a core Python package or on PyPi, put it here.
import datetime
from dateutil import tz
import time
from sqlalchemy import text
from ast import literal_eval
from collections import OrderedDict

# Airflow Base Classes
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Airflow Extended Classes 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from salesforce_plugin.hooks.salesforce_hook import SalesforceHook
from hubspot_plugin.hooks.hubspot_hook import HubspotHook

class PostgresToHubspotOperator(BaseOperator):
    """
    Executes a query from S3 and upserts the results to Hubspot
    :param query_s3_bucket: reference to a specific S3 bucket to retrieve sql
    :type query_s3_bucket: string
    :param query_s3_key: reference to a specific S3 key to retrieve sql
    :type query_s3_key: string
    :param database_conn_id: reference to a specific redshift database
    :type database_conn_id: string
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: string
    :param sql_params: allows for parameterization of SQL according to sqlalchemy docs;
        e.g. 'WHERE id = :id' in SQL and pass {'id': 1} will parameterize :id as 1 (templated)
    :type sql_params: dictionary
    """

    template_fields = ('sql_params',)
    template_ext = dict()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            query_s3_bucket,
            query_s3_key,
            database_conn_id='database_default',
            aws_conn_id='aws_default',
            hubspot_conn_id='hubspot_default',
            sql_params={},
            *args, **kwargs):
        super(PostgresToHubspotOperator, self).__init__(*args, **kwargs)
        self.query_s3_bucket = query_s3_bucket
        self.query_s3_key = query_s3_key
        self.database_conn_id = database_conn_id
        self.aws_conn_id = aws_conn_id
        self.hubspot_conn_id = hubspot_conn_id
        self.sql_params = sql_params

    def execute(self, context):
        utc = tz.gettz('UTC')
        eastern = tz.gettz('America/New_York')

        if type(self.sql_params) is str:
            self.sql_params = literal_eval(self.sql_params)

        hubspot = HubspotHook(conn_id=self.hubspot_conn_id)
        database = PostgresHook(postgres_conn_id=self.database_conn_id)
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)

        s3_object = s3.get_key(
            key=self.query_s3_key,
            bucket_name=self.query_s3_bucket
            )
        query = text(s3_object.get()['Body'].read().decode('utf-8'))

        engine = database.get_sqlalchemy_engine()
        con = engine.connect()
        result = con.execute(query, self.sql_params)

        payload = []
        for row in result:
            row_dict = OrderedDict()
            for column, value in row.items():
                if type(value) == datetime.datetime:
                    value = int(value.replace(tzinfo=utc).timestamp()*1e3)
                    row_dict[column] = value
                if type(value) == datetime.date:
                    value = datetime.datetime.combine(value, datetime.datetime.min.time())
                    value = int(value.replace(tzinfo=utc).timestamp()*1e3)
                    row_dict[column] = value
                else:
                    row_dict[column] = value
            payload.append(row_dict)
        con.close()

        hubspot.upsert_contacts(payload)
        