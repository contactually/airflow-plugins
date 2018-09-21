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

# Airflow Base Classes
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Airflow Extended Classes 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook


class CustomRedshiftToS3Operator(BaseOperator):
    """
    Executes an UNLOAD command to s3 as a CSV with headers
    :param query_s3_bucket: reference to a specific S3 bucket to retrieve sql
    :type query_s3_bucket: string
    :param query_s3_key: reference to a specific S3 key to retrieve sql
    :type query_s3_key: string
    :param dest_s3_bucket: reference to a specific S3 bucket to put files
    :type dest_s3_bucket: string
    :param dest_s3_key: reference to a specific S3 key to put files
    :type dest_s3_key: string
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: string
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: string
    :param unload_options: reference to a list of UNLOAD options
    :type unload_options: list
    :param autocommit: specifies whether DML transactions are committed upon submission
    :type autocommit: bool
    :param parameters: specifies parameters to be passed through to psycopg
    :type: list
    :param headers: specifies headers to be added to query results; must be in correct order as query results
    :type: list
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            query_s3_bucket,
            query_s3_key,
            dest_s3_bucket,
            dest_s3_key,
            redshift_conn_id='redshift_default',
            aws_conn_id='aws_default',
            unload_options=tuple(),
            autocommit=False,
            parameters=None,
            headers=[],
            *args, **kwargs):
        super(CustomRedshiftToS3Operator, self).__init__(*args, **kwargs)
        self.query_s3_bucket = query_s3_bucket
        self.query_s3_key = query_s3_key
        self.dest_s3_bucket = dest_s3_bucket
        self.dest_s3_key = dest_s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.unload_options = unload_options
        self.autocommit = autocommit
        self.parameters = parameters
        self.headers = headers

        if self.headers and \
           'PARALLEL OFF' not in [uo.upper().strip() for uo in unload_options]:
            self.unload_options = list(unload_options) + ['PARALLEL OFF', ]

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        a_key, s_key, _, _ = self.s3._get_credentials(region_name=None)
        unload_options = '\n\t\t\t'.join(self.unload_options)

        s3_object = self.s3.get_key(
            key=self.query_s3_key,
            bucket_name=self.query_s3_bucket
            )
        raw_query = s3_object.get()['Body'].read().decode('utf-8').replace("'", "\\'")
        query_list = []

        if self.headers:
            self.log.info("Retrieving headers...")

            columns = self.headers
            column_names = ', '.join("{0}".format(c) for c in columns)
            column_headers = ', '.join("\\'{0}\\'".format(c) for c in columns)

            query_list.append({'type': 'header', 'query': "select {column_headers}".format(column_headers=column_headers)})
            query_list.append({'type': 'query', 'query': raw_query})
        else:
            query_list.append({'type': 'query', 'query': raw_query})

        for query in query_list:
            unload_query = """
                        UNLOAD ('{select_query}')
                        TO 's3://{s3_bucket}/{s3_key}'
                        with credentials
                        'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                        {unload_options};
                        """.format(select_query=query['query'],
                                   s3_bucket=self.dest_s3_bucket,
                                   s3_key="{key}_{type}_".format(key=self.dest_s3_key, type=query['type']),
                                   access_key=a_key,
                                   secret_key=s_key,
                                   unload_options=unload_options)

            self.log.info('Executing UNLOAD command...')
            self.hook.run(unload_query, self.autocommit, self.parameters)
            self.log.info("UNLOAD command complete...")