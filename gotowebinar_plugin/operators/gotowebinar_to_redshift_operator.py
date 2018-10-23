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
from datetime import datetime, timedelta
import logging

# Airflow Base Classes
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Airflow Extended Classes 
from airflow.hooks.postgres_hook import PostgresHook
from aws_plugin.hooks.redshift_hook import RedshiftHook
from gotowebinar_plugin.hooks.gotowebinar_hook import GoToWebinarHook


class GoToWebinarToRedshiftOperator(BaseOperator):
    """
    Retrieves data from Outreach.io and upserts it into database
    :param from_time: required start of datetime range in ISO8601 UTC format, e.g. 2015-07-13T10:00:00Z
    :type from_time: string
    :param to_time: required end of datetime range in ISO8601 UTC format, e.g. 2015-07-13T22:00:00Z
    :type to_time: string
    :param aws_conn_id: reference to S3
    :type aws_conn_id: string
    :param gotowebinar_conn_id: reference to a specific GoToWebinar connection
    :type gotowebinar_conn_id: string
    :param database_conn_id: reference to a specific database
    :type database_conn_id: string
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            from_time,
            to_time,
            aws_conn_id='aws_default',
            gotowebinar_conn_id='gotowebinar_default',
            database_conn_id='redshift_default',
            *args, **kwargs):
        super(GoToWebinarToRedshiftOperator, self).__init__(*args, **kwargs)
        self.from_time = from_time,
        self.to_time = to_time,
        self.aws_conn_id = aws_conn_id
        self.gotowebinar_conn_id = gotowebinar_conn_id
        self.database_conn_id = database_conn_id

    def execute(self, context):
        database = PostgresHook(postgres_conn_id=self.database_conn_id)
        gotowebinar = GoToWebinarHook(conn_id=self.gotowebinar_conn_id)
        org_key = gotowebinar.org_key
        consumer_key = gotowebinar.consumer_key

        refresh_token, expires_at, access_token = self.obtain_oauth_credentials(org_key, database)
        oauth_response = gotowebinar.consume_refresh_token(refresh_token)
        access_token = self.persist_oauth_credentials(oauth_response, database)

        webinars = gotowebinar.get_webinars(access_token, org_key, self.from_time, self.to_time)

        webinar_payload = []
        if webinars.get('_embedded'):
            for webinar in webinars['_embedded']['webinars']:
                webinar_dict = OrderedDict()
                webinar_dict['webinarkey'] = webinar['webinarKey']
                webinar_dict['starttime'] = webinar['times'][0]['startTime']
                webinar_dict['endtime'] = webinar['times'][0]['endTime']
                webinar_dict['webinarid'] = webinar['webinarId']
                webinar_dict['subject'] = webinar['subject']
                webinar_payload.append(webinar_dict)

        session_payload = []
        registrant_payload = []
        attendee_payload = []
        if webinar_payload:
            for webinar in webinar_payload:
                sessions = gotowebinar.get_sessions(access_token, org_key, webinar['webinarkey'])
                if sessions.get('_embedded'):
                    for session in sessions['_embedded']['sessionInfoResources']:
                        session_dict = OrderedDict()
                        session_dict['sessionkey'] = session['sessionKey']
                        session_dict['webinarkey'] = session['webinarKey']
                        session_dict['webinarid'] = session['webinarID']
                        session_dict['starttime'] = session['startTime']
                        session_dict['endtime'] = session['endTime']
                        session_dict['registrantsattended'] = session['registrantsAttended']
                        session_payload.append(session_dict)

                registrants = gotowebinar.get_registrants(access_token, org_key, webinar['webinarkey'])
                for registrant in registrants:
                    registrant_dict = OrderedDict()
                    registrant_dict['firstname'] = registrant['firstName']
                    registrant_dict['email'] = registrant['email']
                    registrant_dict['lastname'] = registrant['lastName']
                    registrant_dict['registrantkey'] = registrant['registrantKey']
                    registrant_dict['registrationdate'] = registrant['registrationDate']
                    registrant_dict['status'] = registrant['status']
                    registrant_dict['joinurl'] = registrant['joinUrl']
                    registrant_dict['timezone'] = registrant['timeZone']
                    registrant_dict['webinarkey'] = webinar['webinarkey']
                    registrant_payload.append(registrant_dict)

                if session_payload:
                    for session in session_payload:
                        attendees = gotowebinar.get_attendees(access_token, org_key, webinar['webinarkey'], session['sessionkey'])
                        for attendee in attendees:
                            attendee_dict = OrderedDict()
                            try:
                                attendee_dict['registrantkey'] = attendee['registrantKey']
                                attendee_dict['sessionkey'] = attendee['sessionKey']
                                attendee_dict['firstname'] = attendee['firstName']
                                attendee_dict['lastname'] = attendee['lastName']
                                attendee_dict['email'] = attendee['email']
                                attendee_dict['attendancetime'] = attendee['attendanceTimeInSeconds']
                                attendee_dict['jointime'] = attendee['attendance'][0]['joinTime']
                                attendee_dict['leavetime'] = attendee['attendance'][0]['leaveTime']
                                attendee_payload.append(attendee_dict)
                            except TypeError:
                                logging.error("Error record: {record}".format(record=attendee))
                                continue

        self.upsert(database, webinar_payload, 'gotowebinar.webinar', 'webinarkey')
        self.upsert(database, session_payload, 'gotowebinar.session', 'sessionkey')
        self.upsert(database, registrant_payload, 'gotowebinar.registrant', 'registrantkey')
        self.upsert(database, attendee_payload, 'gotowebinar.attendee', 'registrantkey')

    def obtain_oauth_credentials(self, org_key, database):
        query = """
        select 
          expires_at,
          refresh_token,
          access_token
        from gotowebinar.oauth_credentials
        where organizer_key = '{org_key}'
        """.format(org_key=org_key)

        engine = database.get_sqlalchemy_engine()
        con = engine.connect()
        result = con.execute(query)

        refresh_token = None
        expires_at = None
        access_token = None
        for row in result:
            expires_at = row['expires_at']
            refresh_token = row['refresh_token']
            access_token = row['access_token']
        return refresh_token, expires_at, access_token

    def persist_oauth_credentials(self, oauth_response, database):
        access_token = oauth_response.get('access_token')
        expires_in = int(oauth_response.get('expires_in'))
        refresh_token = oauth_response.get('refresh_token')
        version = oauth_response.get('version')
        account_key = oauth_response.get('account_key')
        account_type = oauth_response.get('account_type')
        email = oauth_response.get('email')
        first_name = oauth_response.get('firstName')
        last_name = oauth_response.get('lastName')
        organizer_key = oauth_response.get('organizer_key')

        query = """
        update gotowebinar.oauth_credentials
        set access_token = '{access_token}',
        expires_in = '{expires_in}',
        refresh_token = '{refresh_token}',
        version = '{version}',
        account_key = '{account_key}',
        account_type = '{account_type}',
        email = '{email}',
        firstname = '{first_name}',
        lastname = '{last_name}',
        updated_at = current_timestamp,
        expires_at = dateadd(second, {expires_in}, current_date)
        where organizer_key = '{organizer_key}'
        """.format(
            access_token=access_token,
            expires_in=expires_in,
            refresh_token=refresh_token,
            version=version,
            account_key=account_key,
            account_type=account_type,
            email=email,
            first_name=first_name,
            last_name=last_name,
            organizer_key=organizer_key)

        database.run(query)
        return access_token

    def upsert(self, database, payload, target_table, primary_key):
        logging.info('Upserting {payload} records into {target_table}.'.format(payload=len(payload), target_table=target_table))
        engine = database.get_sqlalchemy_engine()
        con = engine.connect() 

        temp_table_string = 'create temp table staging (like {target_table});'.format(target_table=target_table)
        insert_string = self.create_insert_string(payload)
        insert_string = 'insert into staging values {insert_string};'.format(insert_string=insert_string)
        upsert_string = """
        delete from {target_table} using staging
        where {target_table}.{primary_key} = staging.{primary_key};
        insert into {target_table} select * from staging;
        drop table staging;
        """.format(target_table=target_table, primary_key=primary_key)

        query = temp_table_string + insert_string + upsert_string

        result = con.execute(text(query))
        result.close()

        logging.info('Upsert complete!')

    def create_insert_string(self, payload):
        insert_array = []
        for record in payload:
            v_array = []
            for key, value in record.items():
                if isinstance(value, int) or isinstance(value, float):
                    value = str(value)
                    v_array.append(value)
                elif isinstance(value, list):
                    value = "'{value}'".format(value=','.join(value).replace("'","").replace("%","").replace(":","")) if value else 'NULL'
                    v_array.append(value)
                else:
                    value = "'{value}'".format(value=str(value).replace("'","").replace("%","").replace(":","")) if value else 'NULL'
                    v_array.append(value)
            insert_values = '({insert_values})'.format(insert_values=','.join(v_array))
            insert_array.append(insert_values)
        insert_string = ','.join(insert_array)
        return insert_string
        