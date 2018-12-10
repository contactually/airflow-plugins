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
from collections import OrderedDict
from sqlalchemy import text

# Airflow Base Classes
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Airflow Extended Classes 
from airflow.hooks.postgres_hook import PostgresHook
from aws_plugin.hooks.redshift_hook import RedshiftHook
from zoom_plugin.hooks.zoom_hook import ZoomHook


class ZoomWebinarToRedshiftOperator(BaseOperator):
    """
    Retrieves data from Zoom and upserts it into database
    :param user_id: Zoom user ID to fetch data for
    :type user_id: string
    :param target_table: table to upsert records to
    :type target_table: string
    :param aws_conn_id: reference to S3
    :type aws_conn_id: string
    :param zoom_conn_id: reference to a specific Zoom connection
    :type zoom_conn_id: string
    :param database_conn_id: reference to a specific database
    :type database_conn_id: string
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            user_id,
            schema,
            target_table,
            field_list,
            aws_conn_id='aws_default',
            zoom_conn_id='zoom_default',
            database_conn_id='redshift_default',
            *args, **kwargs):
        super(ZoomWebinarToRedshiftOperator, self).__init__(*args, **kwargs)
        self.user_id = user_id
        self.schema = schema
        self.target_table = target_table
        self.field_list = field_list
        self.aws_conn_id = aws_conn_id
        self.zoom_conn_id = zoom_conn_id
        self.database_conn_id = database_conn_id

    def execute(self, context):
        database = PostgresHook(postgres_conn_id=self.database_conn_id)
        zoom = ZoomHook(zoom_conn_id=self.zoom_conn_id)

        webinar_payload = zoom.list_webinars(self.user_id)

        if self.target_table == 'registrant':
            registrant_payload = []
            for record in webinar_payload:
                registrants = zoom.list_registrants(record['id'])
                for registrant in registrants:
                    registrant['webinar_id'] = record['id']
                    registrant_payload.append(registrant)

        if self.target_table == 'participant':
            participant_payload = []
            for record in webinar_payload:
                participants = zoom.list_participants(record['id'])
                for participant in participants:
                    participant['webinar_id'] = record['id']
                    participant_payload.append(participant)

        payload = eval("{target_table}_payload".format(target_table=self.target_table))

        try:
            self.upsert(database, payload, self.schema, self.target_table, self.field_list)
        except Exception as e:
            self.log.info(str(e))

    def upsert(self, database, payload, schema, target_table, field_list):
        ordered_payload = []
        for record in payload:
            ordered_record = OrderedDict()
            for field_name in self.field_list:
                ordered_record[field_name] = record.get(field_name)
            ordered_payload.append(ordered_record)

        self.log.info('Upserting {payload} records into {schema}.{target_table}.'.format(payload=len(payload), schema=schema, target_table=target_table))
        engine = database.get_sqlalchemy_engine()
        con = engine.connect() 

        temp_table_string = 'create table {schema}.{target_table}_staging (like {schema}.{target_table});'.format(schema=schema, target_table=target_table)
        insert_string = self.create_insert_string(ordered_payload)
        insert_string = 'insert into {schema}.{target_table}_staging values {insert_string};'.format(schema=schema, target_table=target_table, insert_string=insert_string)
        upsert_string = """
        drop table if exists {schema}.{target_table};
        alter table {schema}.{target_table}_staging rename to {target_table};
        alter table {schema}.{target_table} owner to airflow;
        """.format(schema=schema, target_table=target_table)

        query = temp_table_string + insert_string + upsert_string

        result = con.execute(text(query))
        result.close()

        self.log.info('Upsert complete!')

    def create_insert_string(self, payload):
        insert_array = []
        for record in payload:
            v_array = []
            for key, value in record.items():
                if isinstance(value, int) or isinstance(value, float):
                    value = str(value)[:499]
                    v_array.append(value)
                elif isinstance(value, list):
                    value = "'{value}'".format(value=','.join(value).replace("'","").replace("%","").replace(":","")[:300]) if value else 'NULL'
                    v_array.append(value)
                else:
                    value = "'{value}'".format(value=str(value).replace("'","").replace("%","").replace(":","")[:300]) if value else 'NULL'
                    v_array.append(value)
            insert_values = '({insert_values})'.format(insert_values=','.join(v_array))
            insert_array.append(insert_values)
        insert_string = ','.join(insert_array)
        return insert_string
        