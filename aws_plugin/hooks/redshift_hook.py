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
"""
This module contains a Salesforce Hook based off of the contrib version
which allows you to connect to your Salesforce instance,
retrieve data from it, and write data back to Salesforce.
NOTE:   this hook also relies on the simple_salesforce package:
        https://github.com/simple-salesforce/simple-salesforce
"""

# Widely Available Packages -- if it's a core Python package or on PyPi, put it here.
import json
import time
import re

# Airflow Base Classes
from airflow.utils.log.logging_mixin import LoggingMixin

# Airflow Extended Classes
from airflow.contrib.hooks.aws_hook import AwsHook


class RedshiftHook(AwsHook, LoggingMixin):
    """
    Interact with Redshift via the boto3 library.
    """

    def get_conn(self):
        return self.get_client_type('redshift')

    def create_cluster_snapshot(self, snapshot_identifier, cluster_identifier, tags=[]):
        """
        :param snapshot_identifier: unique identifier for the snapshot that you are requesting
        :type snapshot_identifiier: str
        :param cluster_identifier: cluster identifier for which you want a snapshot
        :type cluster_identifier: str
        :param tags: list of tag instances
        :type tags: list of dicts
        """

        client = self.get_conn()
        options = {"SnapshotIdentifier": snapshot_identifier, "ClusterIdentifier": cluster_identifier, "Tags": tags}
        response = client.create_cluster_snapshot(**options)

        snapshot_status_options = {"SnapshotIdentifier": snapshot_identifier, "SnapshotType": 'manual'}

        snapshot_complete = False
        while not snapshot_complete:
            self.log.info('Creating snapshot...')
            time.sleep(10)
            status = client.describe_cluster_snapshots(**snapshot_status_options)
            status = status['Snapshots'][0].get('Status')
            if status == None:
                self.log.error('Error creating cluster')
                break
            elif status == 'available':
                snapshot_complete = True
                break
            else:
                continue

        return self.log.info('Snapshot generated!')

    def delete_cluster_snapshots(self, cluster_identifier, snapshot_type, start_time, end_time):
        """
        :param cluster_identifier: cluster identifier for which you want a snapshot
        :type cluster_identifier: str
        :param snapshot_type: type of snapshot to delete
        :type tags: str
        :param start_time: request snapshots created at or after specified time
        :type start_time: datetime
        :param end_time: request snapshots created before specified time
        :type end_time: datetime
        """

        client = self.get_conn()
        options = {"ClusterIdentifier": cluster_identifier, "SnapshotType": snapshot_type, "StartTime": start_time, "EndTime": end_time}

        response = client.describe_cluster_snapshots(**options)
        snapshots_for_deletion = []
        if not response['Snapshots']:
            for snapshot in response['Snapshots']:
                snapshot_identifier = snapshot.get('SnapshotIdentifier')
                snapshots_for_deletion.append(snapshot_identifier)

        if not snapshots_for_deletion:
            for snapshot_identifier in snapshots_for_deletion:
                self.log.info("Deleting snapshot {snapshot}".format(snapshot=snapshot_identifier))
                options = {"SnapshotIdentifier": snapshot_identifier, "ClusterIdentifier": cluster_identifier}
                client.delete_cluster_snapshot(**options)

        return self.log.info('All {type} snapshots between {start_time} and {end_time} deleted!'.format(type=snapshot_type, start_time=start_time, end_time=end_time))


