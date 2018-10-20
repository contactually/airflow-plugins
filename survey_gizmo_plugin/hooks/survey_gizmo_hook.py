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
#

# Widely Available Packages -- if it's a core Python package or on PyPi, put it here.
from surveygizmo import SurveyGizmo
from datetime import datetime, timedelta
from dateutil import tz, parser
from collections import OrderedDict
import json

# Airflow Base Classes
from airflow.utils.log.logging_mixin import LoggingMixin

# Airflow Extended Classes
from airflow.hooks.base_hook import BaseHook


class SurveyGizmoHook(BaseHook, LoggingMixin):
    def __init__(
            self,
            survey_gizmo_conn_id,
            *args,
            **kwargs
    ):
        """
        Create new connection to Zuora
        and allows you to pull data out of Zuora.
        :param zuora_conn_id:     the name of the connection that has the parameters
                            we need to connect to Zuora.
                            The connection shoud be type `http` and include the API endpoint in the `Extras` field.
        .. note::
            For the HTTP connection type, you can include a
            JSON structure in the `Extras` field.
            We need the appropriate API endpoint for production vs sandbox.
            So we define it in the `Extras` field as:
                `{"endpoint":"ENDPOINT"}`
        """
        self.survey_gizmo_conn_id = survey_gizmo_conn_id
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(survey_gizmo_conn_id)
        self.extras = self.connection.extra_dejson

    def sign_in(self):
        """
        Sign into Zuora.
        If we have already signed it, this will just return the original object
        """
        if hasattr(self, 'gizmo'):
            return self.gizmo

        gizmo = SurveyGizmo(
            api_version=self.extras.get('api_version', 'v5'),
            api_token=self.extras.get('api_token'),
            api_token_secret=self.extras.get('api_token_secret')
        )
        self.gizmo = gizmo
        return gizmo

    def get_survey_data(self, survey_id):
        """
        Queries SurveyGizmo surveys and returns information about survey specified.
        :param survey_id: survey ID to retrieve data for
        :type survey_id: string
        """

        self.sign_in()

        survey = self.gizmo.api.survey.get(survey_id)
        survey_payload = []
        survey_dict = OrderedDict()
        survey_dict['id'] = survey_id
        survey_dict['title'] = survey['data']['title']
        survey_dict['partial'] = survey['data']['statistics'].get('Partial', 0)
        survey_dict['deleted'] = survey['data']['statistics'].get('Deleted', 0)
        survey_dict['complete'] = survey['data']['statistics'].get('Complete', 0)
        survey_payload.append(survey_dict)

        question = self.gizmo.api.surveyquestion.list(survey_id)

        question_payload = []
        option_payload = []
        if question['data']:
            for record in question['data']:
                record_dict = OrderedDict()
                if record['base_type'] == 'Question':
                    record_dict['id'] = record['id']
                    record_dict['survey_id'] = survey_id
                    record_dict['question_title'] = record['title']['English']
                    record_dict['type'] = record['type'].lower()
                    question_payload.append(record_dict)
                    for option in record['options']:
                        option_dict = OrderedDict()
                        option_dict['id'] = option['id']
                        option_dict['question_id'] = record['id']
                        option_dict['survey_id'] = survey_id
                        option_dict['value'] = option['value']
                        option_payload.append(option_dict)

        return survey_payload, question_payload, option_payload

    def get_survey_response_data(self, survey_id, filter, filter_field, filter_operator, filter_value):
        """
        Queries SurveyGizmo survey responses and returns information about survey specified.
        :param survey_id: survey ID to retrieve data for
        :type survey_id: string
        :param filter: tells whether to use a filter on the call
        :type filter: boolean
        :param filter_field: which type of filter to use according to the API documentation.
        :type filter_field: string
        :param filter_comparison: the comparison type to use.
        :type filter_comparison: string
        :param filter_value: the value to use for filtering.
        :type filter_value: string 
        """

        self.sign_in()

        if filter:
            responses = self.gizmo.api.surveyresponse.filter(field=filter_field, operator=filter_operator, value=filter_value)
        else:
            responses = self.gizmo.api.surveyresponse

        utc = tz.gettz('UTC')
        page = 1
        total_pages = 1
        payload = []
        while page <= total_pages:
            responses = responses.page('{page}'.format(page=page)).list(survey_id)
            for response in responses['data']:
                for key, value in response['survey_data'].items():
                    if value['type'] == 'GDATASPREADSHEET':
                        continue
                    if value.get('options'):
                        for k, v in value['options'].items():
                            response_dict = OrderedDict()
                            response_dict['id'] = str(response['id']) + str(survey_id) + str(value['id']) + str(v['id'])
                            response_dict['response_id'] = response['id']
                            response_dict['survey_id'] = survey_id
                            response_dict['question_id'] = value['id']
                            response_dict['option_id'] = v['id']
                            response_dict['answer_text'] = value.get('answer')
                            response_dict['submitted_at'] = parser.parse(response['date_submitted']).strftime("%Y-%m-%d %H:%M:%S") if response.get('date_submitted') else None
                            payload.append(response_dict)
                    else:
                        response_dict = OrderedDict()
                        response_dict['id'] = str(response['id']) + str(survey_id) + str(value['id']) + '0'
                        response_dict['response_id'] = response['id']
                        response_dict['survey_id'] = survey_id
                        response_dict['question_id'] = value['id']
                        response_dict['option_id'] = None
                        response_dict['answer_text'] = value.get('answer')
                        response_dict['submitted_at'] = parser.parse(response['date_submitted']).strftime("%Y-%m-%d %H:%M:%S") if response.get('date_submitted') else None
                        payload.append(response_dict)

            total_pages = responses['total_pages']
            page += 1

        return payload
