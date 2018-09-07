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

# Airflow Base Classes
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Airflow Extended Classes 
from airflow.hooks.postgres_hook import PostgresHook
from salesforce_plugin.hooks.salesforce_hook import SalesforceHook
from airflow.hooks.S3_hook import S3Hook

class SalesforceUpsertOperator(BaseOperator):
    """
    Executes a query from S3 and upserts the results to Salesforce
    :param salesforce_object: Salesforce object to perform upsert on
    :type salesforce_object: string
    :param upsert_field: field used as key for upsert
    :type upsert_field: string
    :param query_s3_bucket: reference to a specific S3 bucket to retrieve sql
    :type query_s3_bucket: string
    :param query_s3_key: reference to a specific S3 key to retrieve sql
    :type query_s3_key: string
    :param database_conn_id: reference to a specific redshift database
    :type database_conn_id: string
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: string
    :param salesforce_conn_id: reference to a specific Salesforce connection
    :type salesforce_conn_id: string
    :param no_null_list: list of field names to avoid blanking out if query returns NULL
    :type no_null_list: list
    :param lookup_mapping: list of dictionaries with key-value pair indicating the field name 
        and external field name used for lookup in the form {lookup_field_name__c: external_field_name__c}
    :type lookup_mapping: list of dictionaries
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            salesforce_object,
            upsert_field,
            query_s3_bucket,
            query_s3_key,
            database_conn_id='database_default',
            aws_conn_id='aws_default',
            salesforce_conn_id='salesforce_default',
            no_null_list=[],
            lookup_mapping={},
            *args, **kwargs):
        super(SalesforceUpsertOperator, self).__init__(*args, **kwargs)
        self.salesforce_object = salesforce_object
        self.upsert_field = upsert_field
        self.query_s3_bucket = query_s3_bucket
        self.query_s3_key = query_s3_key
        self.database_conn_id = database_conn_id
        self.aws_conn_id = aws_conn_id
        self.salesforce_conn_id = salesforce_conn_id
        self.no_null_list = no_null_list
        self.lookup_mapping = lookup_mapping

    def execute(self, context):
        self.database = PostgresHook(postgres_conn_id=self.database_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        self.salesforce = SalesforceHook(conn_id=self.salesforce_conn_id)

        s3_object = self.s3.get_key(
            key=self.query_s3_key,
            bucket_name=self.query_s3_bucket
            )
        query = s3_object.get()['Body'].read().decode('utf-8')

        engine = self.database.get_sqlalchemy_engine()
        con = engine.connect()
        result = con.execute(query)

        records = []
        mapping = {k.lower(): v.lower() for k, v in self.lookup_mapping.items()}
        for row in result:
            row_dict = {}
            for column, value in row.items():
                if value == None and column in [x.lower() for x in self.no_null_list]:
                    next
                elif type(value) == datetime.datetime:
                    value = value.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    row_dict[column] = value
                elif value == None and column.endswith('__r'):
                    row_dict[column.replace('__r', '__c')] = value
                elif column in list(mapping):
                    row_dict[column] = {mapping[column]: value}
                else:
                    row_dict[column] = value
            records.append(row_dict)

        self.log.info("Upsert operation on {salesforce_object} beginning...".format(salesforce_object=self.salesforce_object))
        self.salesforce.upsert(self.salesforce_object, self.upsert_field, records)
        self.log.info("Upsert operation on {salesforce_object} complete!".format(salesforce_object=self.salesforce_object))