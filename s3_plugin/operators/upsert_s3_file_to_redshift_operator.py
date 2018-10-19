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
from io import StringIO
import csv
from collections import OrderedDict
from datetime import datetime,timedelta
from dateutil import tz
from sqlalchemy import text

# Airflow Base Classes
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Airflow Extended Classes 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook


class UpsertS3FileToRedshiftOperator(BaseOperator):
    """
    Executes a .sql query from S3 in Redshift in an UNLOAD command back to S3
    :param s3_bucket: reference to a specific S3 bucket to retrieve file
    :type s3_bucket: string
    :param s3_key: reference to a specific S3 key to retrieve file
    :type s3_key: string
    :param target_table: table to upsert data to
    :type target_table: string
    :param primary_key: primary key to use in upsert operation
    :type primary_key: string
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
            file_delimiter,
            target_table,
            primary_key,
            redshift_conn_id='redshift_default',
            aws_conn_id='aws_default',
            aws_region=None,
            autocommit=False,
            *args, **kwargs):
        super(UpsertS3FileToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_delimiter = file_delimiter
        self.target_table = target_table
        self.primary_key = primary_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.aws_region = aws_region
        self.autocommit = autocommit

    def execute(self, context):
        database = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        a_key, s_key, _, _ = s3._get_credentials(region_name=self.aws_region)

        s3_object = s3.get_key(
            key=self.s3_key,
            bucket_name=self.s3_bucket
            )
        raw_file = s3_object.get()['Body'].read().decode('utf-8')

        payload = []
        file = StringIO(raw_file)
        reader = csv.reader(file, delimiter=self.file_delimiter)
        headers = next(reader)
        for row in reader:
            payload.append(OrderedDict(zip(headers, row)))

        self.upsert(database=database, payload=payload)

    def upsert(self, database, payload):
        self.log.info('Upserting {payload} records into {target_table}.'.format(payload=len(payload), target_table=self.target_table))
        engine = database.get_sqlalchemy_engine()
        con = engine.connect() 

        temp_table_string = 'create temp table staging (like {target_table});'.format(target_table=self.target_table)
        insert_string = self.create_insert_string(payload)
        insert_string = 'insert into staging values {insert_string};'.format(insert_string=insert_string)
        upsert_string = """
        delete from {target_table} using staging
        where {target_table}.{primary_key} = staging.{primary_key};
        insert into {target_table} select * from staging;
        drop table staging;
        """.format(target_table=self.target_table, primary_key=self.primary_key)

        query = temp_table_string + insert_string + upsert_string

        result = con.execute(text(query))
        result.close()

        self.log.info('Upsert complete!')

    def create_insert_string(self, payload):
        utc = tz.gettz('UTC')

        insert_array = []
        for record in payload:
            v_array = []
            for key, value in record.items():
                if isinstance(value, int) or isinstance(value, float):
                    value = str(value)[:499]
                    v_array.append(value)
                elif isinstance(value, list):
                    value = "'{value}'".format(value=','.join(value).replace("'","")[:300]) if value else 'NULL'
                    v_array.append(value)
                elif isinstance(value, datetime):
                    value = "'{value}'".format(value=value.astimezone(utc).strftime("%Y-%m-%d %H:%M:%S"))
                    value_array.append(value)
                else:
                    value = "'{value}'".format(value=str(value).replace("'","")[:300]) if value else 'NULL'
                    v_array.append(value)
            insert_values = '({insert_values})'.format(insert_values=','.join(v_array))
            insert_array.append(insert_values)
        insert_string = ','.join(insert_array)
        return insert_string
        