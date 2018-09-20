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
import zeep
from datetime import datetime,timedelta
from dateutil import tz
from collections import OrderedDict

# Airflow Base Classes
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Airflow Extended Classes 
from zuora_plugin.hooks.zuora_rest_hook import ZuoraRestHook
from zuora_plugin.hooks.zuora_soap_hook import ZuoraSoapHook
from airflow.hooks.postgres_hook import PostgresHook


class ZuoraToRedshiftOperator(BaseOperator):
    """
    Runs ZOQL query in Zuora and upserts results into Redshift
    :param zuora_query: ZOQL string to query Zuora
    :type zuora_query: string
    :param target_table: table in target Redshift DB that process will write to
    :type target_table: string
    :param primary_key: primary key for target table
    :type primary_key: string
    :param field_list: list of fields to restrict update values to
    :type field_list: list
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: string
    :param zuora_conn_id: reference to a specific Zuora connection
    :type zuora_conn_id: string
    :param use_rest_api: indicates whether to connect to Zuora via REST or SOAP
    :type use_rest_api: bool
    :param zuora_soap_wsdl: location of WSDL for SOAP connection
    :type zuora_soap_wsdl: string
    :param autocommit: specifies whether DML transactions are committed upon submission
    :type autocommit: bool
    """

    template_fields = ('zuora_query','zuora_soap_wsdl')
    template_ext = str()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            zuora_query,
            target_table,
            primary_key,
            field_list,
            redshift_conn_id='redshift_default',
            zuora_conn_id='zuora_default',
            use_rest_api=True,
            zuora_soap_wsdl=None,
            autocommit=True,
            *args, **kwargs):
        super(ZuoraToRedshiftOperator, self).__init__(*args, **kwargs)
        self.zuora_query = zuora_query
        self.target_table = target_table
        self.primary_key = primary_key
        self.field_list = field_list
        self.redshift_conn_id = redshift_conn_id
        self.zuora_conn_id = zuora_conn_id
        self.use_rest_api = use_rest_api
        self.zuora_soap_wsdl = zuora_soap_wsdl
        self.autocommit = autocommit

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.use_rest_api:
            zuora = ZuoraRestHook(zuora_conn_id=self.zuora_conn_id)
        else:
            zuora = ZuoraSoapHook(zuora_conn_id=self.zuora_conn_id, wsdl_url=self.zuora_soap_wsdl)

        self.log.info("Generating Zuora payload...")
        payload = zuora.query(self.zuora_query)
        create_staging_temp = "create temp table staging (like {target});".format(target=self.target_table)
        utc = tz.gettz('UTC')

        if not payload[0]['Id'] and len(payload) == 1:
            return self.log.info('No records needed to be updated!')

        insert_array = []
        for record in payload:
            value_array = []
            if not self.use_rest_api:
                record = zeep.helpers.serialize_object(record, target_cls=dict)

            ordered_record = OrderedDict()
            for field in self.field_list:
                ordered_record[field] = record[field]

            for _key, value in ordered_record.items():
                if _key not in self.field_list:
                    continue
                elif isinstance(value, int) or isinstance(value, float):
                    value_array.append(str(value))
                elif isinstance(value, list):
                    if not value:
                        value_array.append('NULL')
                    else:
                        value = "'{value}'".format(value=",".join(value).replace("'", ""))
                        value_array.append(value)
                elif isinstance(value, datetime):
                    value = "'{value}'".format(value=value.astimezone(utc).strftime("%Y-%m-%d %H:%M:%S"))
                    value_array.append(value)
                else:
                    if not value:
                        value_array.append('NULL')
                    else:
                        value = "'{value}'".format(value=str(value).replace("'",""))
                        value_array.append(value)
            insert_value = "({insert_value})".format(insert_value=",".join(value_array))
            insert_array.append(insert_value)
        insert_string = ",".join(insert_array)
        insert_staging_temp = "insert into staging values {insert_string};".format(insert_string=insert_string)

        sql_query = """
        {create_staging_temp}
        {insert_staging_temp}
        delete from {target} using staging where {target}.{primary_key} = staging.{primary_key};
        insert into {target} select * from staging;
        drop table staging;
        """.format(create_staging_temp=create_staging_temp, insert_staging_temp=insert_staging_temp, target=self.target_table, primary_key=self.primary_key)

        self.log.info("Beginning Redshift upsert...")
        redshift.run(sql_query, self.autocommit)
        self.log.info("Redshift upsert complete to {target_table}!".format(target_table=self.target_table))