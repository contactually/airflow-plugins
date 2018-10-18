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
from sqlalchemy import text
import json
import csv
from tempfile import NamedTemporaryFile
import os
from collections import OrderedDict

# Airflow Base Classes
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Airflow Extended Classes 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook


class S3QueryToLambdaOperator(BaseOperator):
    """
    Executes a .sql file from S3 in Redshift, sends results to Lambda, then stores results back in S3
    :param query_s3_bucket: reference to a specific S3 bucket to retrieve sql
    :type query_s3_bucket: string
    :param query_s3_key: reference to a specific S3 key to retrieve sql
    :type query_s3_key: string
    :param dest_s3_bucket: reference to a specific S3 bucket to put files
    :type dest_s3_bucket: string
    :param dest_s3_key: reference to a specific S3 key to put files
    :type dest_s3_key: string
    :param function_name: name of AWS Lambda function
    :type function_name: string
    :param aws_region: name of AWS region
    :type aws_region: string
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: string
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: string
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
            function_name,
            aws_region,
            redshift_conn_id='redshift_default',
            aws_conn_id='aws_default',
            *args, **kwargs):
        super(S3QueryToLambdaOperator, self).__init__(*args, **kwargs)
        self.query_s3_bucket = query_s3_bucket
        self.query_s3_key = query_s3_key
        self.dest_s3_bucket = dest_s3_bucket
        self.dest_s3_key = dest_s3_key
        self.function_name = function_name
        self.aws_region = aws_region
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        self.database = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        self.awslambda = AwsLambdaHook(aws_conn_id=self.aws_conn_id, function_name=self.function_name, region_name=self.aws_region)

        s3_object = self.s3.get_key(
            key=self.query_s3_key,
            bucket_name=self.query_s3_bucket
            )     

        self.log.info('Executing query...')
        query = s3_object.get()['Body'].read().decode('utf-8')

        engine = self.database.get_sqlalchemy_engine()
        con = engine.connect()
        result = con.execute(query)
        self.log.info('Query complete, sending results to Lambda function {function_name}...'.format(function_name=self.function_name))

        records = []
        for row in result:
            row_list = []
            for column, value in row.items():
                value = 0 if value == float('inf') or value == float('-inf') else value
                row_list.append(value)
            records.append(row_list)

        payload = json.dumps({"input": records})
        lambda_result = self.awslambda.invoke_lambda(payload)

        results = json.loads(lambda_result['Payload'].read().decode('utf-8'), object_pairs_hook=OrderedDict)

        f_source = NamedTemporaryFile(mode='w+t', suffix='.csv', delete=False)
        fieldnames = [k for k in results[0].keys()]
        writer = csv.DictWriter(f_source, fieldnames=fieldnames, delimiter='|')

        writer.writeheader()
        for row in results:
            writer.writerow(row)
        
        f_source.close()   
        self.s3.load_file(filename=f_source.name, key=self.dest_s3_key, bucket_name=self.dest_s3_bucket, replace=True)
        self.log.info("File loaded to S3.")
        os.remove(f_source.name)