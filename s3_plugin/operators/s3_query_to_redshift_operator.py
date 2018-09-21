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


class S3QueryToRedshiftOperator(BaseOperator):
    """
    Reads a .sql file from S3 and executes it in Redshift
    :param s3_bucket: reference to a specific S3 bucket where .sql file is stored
    :type s3_bucket: string
    :param s3_key: reference to a specific S3 key identifying .sql file
    :type s3_key: string
    :param process_name: custom process name for logs
    :type process_name: string
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: string
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: string
    :param autocommit: specifies whether DML transactions are committed upon submission
    :type autocommit: bool
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            s3_bucket,
            s3_key,
            process_name='SQL',
            redshift_conn_id='redshift_default',
            aws_conn_id='aws_default',
            autocommit=True,
            *args, **kwargs):
        super(S3QueryToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.process_name = process_name
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.autocommit = autocommit

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id)

        s3_object = self.s3.get_key(
            key=self.s3_key,
            bucket_name=self.s3_bucket
            )     

        sql_query = s3_object.get()['Body'].read().decode('utf-8')

        self.log.info('Executing {process_name} query...'.format(process_name=self.process_name))
        self.hook.run(sql_query, self.autocommit)
        self.log.info('{process_name} query complete...'.format(process_name=self.process_name))