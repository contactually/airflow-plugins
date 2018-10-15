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
from outreach_plugin.hooks.outreach_hook import OutreachHook


class OutreachToRedshiftOperator(BaseOperator):
    """
    Retrieves data from Outreach.io and upserts it into database
    :param resource: reference to Outreach resource to retrieve data from
    :type resource: string
    :param target_table: table to upsert records to
    :type target_table: string
    :param primary_key: primary key to use during upsert operation
    :type primary_key: string
    :param ordered_field_list: ordered dict of fields for upsert
    :type ordered_field_list: OrderedDict
    :param filter: boolean to tell whether to filter query call to Outreach
    :type filter: bool
    :param filter_field: field on which to apply Outreach filter
    :type filter_field: string
    :param filter_statement: tells how Outreach should filter query based on Outreach API documentation
    :type filter_statement: string
    :param page_limit: limit for rows returned by Outreach API
    :type page_limit: int
    :param page_offset: which page to return
    :type page_offset: int
    :param sort: boolean to tell whether to sort query call to Outreach
    :type sort: bool
    :param sort_statement: tells how Outreach should sort query based on Outreach API documentation
    :type sort_statement: string
    :param aws_conn_id: reference to S3
    :type aws_conn_id: string
    :param outreach_conn_id: reference to a specific Outreach connection
    :type outreach_conn_id: string
    :param database_conn_id: reference to a specific database
    :type database_conn_id: string
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            resource,
            target_table,
            primary_key,
            ordered_field_list,
            filter=False,
            filter_field=None, 
            filter_statement=None, 
            page_limit=100, 
            page_offset=0, 
            sort=False, 
            sort_statement=None,
            aws_conn_id='aws_default',
            outreach_conn_id='outreach_default',
            database_conn_id='redshift_default',
            *args, **kwargs):
        super(OutreachToRedshiftOperator, self).__init__(*args, **kwargs)
        self.resource = resource
        self.target_table = target_table
        self.primary_key = primary_key
        self.ordered_field_list = ordered_field_list
        self.filter = filter
        self.filter_field = filter_field
        self.filter_statement = filter_statement
        self.page_limit = page_limit
        self.page_offset = page_offset
        self.sort = sort
        self.sort_statement = sort_statement
        self.aws_conn_id = aws_conn_id
        self.outreach_conn_id = outreach_conn_id
        self.database_conn_id = database_conn_id

    def execute(self, context):
        database = PostgresHook(postgres_conn_id=self.database_conn_id)
        outreach = OutreachHook(outreach_conn_id=self.outreach_conn_id)
        outreach_client_id = outreach.client_id

        refresh_token = self.obtain_oauth_credentials(outreach_client_id, database)
        oauth_response = outreach.consume_refresh_token(refresh_token)
        access_token = self.persist_oauth_credentials(oauth_response, database, outreach_client_id)

        response = outreach.retrieve_all(access_token, self.resource, self.filter, self.filter_field, self.filter_statement, self.page_limit, self.page_offset, self.sort, self.sort_statement)
        payload = []
        if not response:
            self.log.info('No records returned!')
        else:
            for record in response:
                record_payload = OrderedDict()
                record_keys = set(record.keys())
                field_list = set(self.ordered_field_list)
                diff = field_list - record_keys
                for key in diff:
                    record[key] = None
                for key in self.ordered_field_list:
                    record_payload[key] = record[key]
                payload.append(record_payload)
            self.upsert(database, payload)

    def obtain_oauth_credentials(self, client_id, database):
        query = """
        select refresh_token
        from outreach.oauth_credentials
        where client_id = '{client_id}'
        """.format(client_id=client_id)

        engine = database.get_sqlalchemy_engine()
        con = engine.connect()
        result = con.execute(query)

        refresh_token = ''
        for row in result:
            refresh_token = row['refresh_token']
        return refresh_token

    def persist_oauth_credentials(self, oauth_response, database, client_id):
        access_token = oauth_response['access_token']
        token_type = oauth_response['token_type']
        expires_in = int(oauth_response['expires_in'])
        refresh_token = oauth_response['refresh_token']
        scope = oauth_response['scope']

        query = """
        update outreach.oauth_credentials
        set access_token = '{access_token}',
        token_type = '{token_type}',
        expires_in = '{expires_in}',
        refresh_token = '{refresh_token}',
        scope = '{scope}',
        updated_at = current_timestamp,
        expires_at = dateadd(second, {expires_in}, current_date)
        where client_id = '{client_id}'
        """.format(
            access_token=access_token,
            token_type=token_type,
            expires_in=expires_in,
            refresh_token=refresh_token,
            scope=scope,
            client_id=client_id)

        database.run(query)
        return access_token

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
        