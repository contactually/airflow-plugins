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
from datetime import datetime,timedelta
from dateutil import tz
import calendar
import time

# Airflow Base Classes
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Airflow Extended Classes 
from zuora_plugin.hooks.zuora_rest_hook import ZuoraRestHook


class ZuoraBillRunOperator(BaseOperator):
    """
    Runs ZOQL query in Zuora and upserts results into Redshift
    :param zuora_conn_id: Connection ID for Zuora
    :type zuora_conn_id: string
    :param invoice_date: Date of invoice in format 'YYYY-MM-DD'
    :type invoice_date: string
    :param target_date: Target date in format 'YYYY-MM-DD'
    :type target_date: string
    :param execute_on_date: Day to create bill run. Use '*' for every day, an integer for day of the month, or 'Last Day of Month'
    :type execute_on_date: string or int
    :param execute_on_timezone: Timezone value used to check whether to create bill run on that day
    :type execute_on_timezone: string
    :param account_id: Zuora account ID if bill run is for one account (optional)
    :type account_id: string
    :param auto_email: Tells Zuora whether to automatically email invoices after bill run is posted
    :type auto_email: bool
    :param auto_post: Tells Zuora whether to automatically post invoices after bill run is created
    :type auto_post: bool
    :param auto_renewal: Tells Zuora whether to automatically renew subscription after bill run is created
    :type auto_renewal: bool
    :param batch: Batch ID for Zuora
    :type batch: string
    :param bill_cycle_day: Which bill cycle days to include in bill run
    :type bill_cycle_day: string or int
    :param charge_type_to_exclude: Which charge types to exclude from bill run
    :type charge_type_to_exclude: string
    :param no_email_for_zero_amount_invoice: Whether to email zero balance invoice customers
    :type no_email_for_zero_amount_invoice: bool
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            zuora_conn_id,
            invoice_date,
            target_date,
            execute_on_day,
            execute_on_timezone='America/New_York',
            account_id=None,
            auto_email=False,
            auto_post=False,
            auto_renewal=False,
            batch='AllBatches',
            bill_cycle_day='AllBillCycleDays',
            charge_type_to_exclude='',
            no_email_for_zero_amount_invoice=False,
            *args, **kwargs):
        super(ZuoraBillRunOperator, self).__init__(*args, **kwargs)
        self.zuora_conn_id = zuora_conn_id
        self.invoice_date = invoice_date
        self.target_date = target_date
        self.execute_on_day = execute_on_day
        self.execute_on_timezone = execute_on_timezone
        self.account_id = account_id
        self.auto_email = auto_email
        self.auto_post = auto_post
        self.auto_renewal = auto_renewal
        self.batch = batch
        self.bill_cycle_day = bill_cycle_day
        self.charge_type_to_exclude = charge_type_to_exclude
        self.no_email_for_zero_amount_invoice = no_email_for_zero_amount_invoice

    def execute(self, context):
        from_zone = tz.gettz('UTC')
        to_zone = tz.gettz(self.execute_on_timezone)
        utc = datetime.utcnow()
        utc = utc.replace(tzinfo=from_zone)
        converted_datetime = utc.astimezone(to_zone)

        run_operator = self.check_run_date(self.execute_on_day, converted_datetime)

        if run_operator:
            self.generate_invoices_and_adjust(
                invoice_date=self.invoice_date,
                target_date=self.target_date,
                account_id=self.account_id,
                auto_email=self.auto_email,
                auto_post=self.auto_post,
                auto_renewal=self.auto_renewal,
                batch=self.batch,
                bill_cycle_day=self.bill_cycle_day,
                charge_type_to_exclude=self.charge_type_to_exclude,
                no_email_for_zero_amount_invoice=self.no_email_for_zero_amount_invoice
                )
        else:
            self.log.info("Execution skipped due to because execute_on_day did not match today.")

    def check_run_date(self, run_day, current_date):
        run_operator = False
        if run_day == '*':
            run_operator = True
        elif isinstance(run_day, int):
            run_operator = True if current_date.day == run_day else False
        elif run_day == 'Last Day of Month':
            last_day_of_month = calendar.monthrange(current_date.year, current_date.month)[1]
            run_operator = True if current_date.day == last_day_of_month else False
        else:
            self.log.error("Invalid execute_on_day parameter: {execute_on_day}".format(execute_on_day=run_day))

        return run_operator

    def generate_invoices_and_adjust(self,
        invoice_date,
        target_date,
        account_id,
        auto_email,
        auto_post,
        auto_renewal,
        batch,
        bill_cycle_day,
        charge_type_to_exclude,
        no_email_for_zero_amount_invoice):
        zuora = ZuoraRestHook(zuora_conn_id=self.zuora_conn_id)

        self.log.info("Creating bill run...")
        result = zuora.create_bill_run(
            invoice_date=invoice_date,
            target_date=target_date,
            account_id=account_id,
            auto_email=auto_email,
            auto_post=auto_post,
            auto_renewal=auto_renewal,
            batch=batch,
            bill_cycle_day=bill_cycle_day,
            charge_type_to_exclude=charge_type_to_exclude,
            no_email_for_zero_amount_invoice=no_email_for_zero_amount_invoice
            )

        if result['Success']:
            bill_run_id = result['Id']
        else:
            self.log.error('Bill run failed!')
        
        bill_run_status_query = """
        select Id, Status
        from BillRun
        where Id = '{bill_run_id}'
        """.format(bill_run_id=bill_run_id)

        status = 'Pending'
        while status in ['Pending', 'PostInProgress']:
            time.sleep(5)
            status_result = zuora.query(bill_run_status_query)
            status = status_result[0]['Status']
        self.log.info("Bill run created. Adjusting negative invoices...")

        negative_invoices_query = """
        select Id, InvoiceNumber, Balance
        from Invoice
        where BillRunId = '{bill_run_id}'
        and Balance < 0
        """.format(bill_run_id=bill_run_id)

        negative_invoices_result = zuora.query(negative_invoices_query)

        if not negative_invoices_result:
            for invoice in negative_invoices_result:
                payload = [{
                'Amount': abs(float(invoice['Balance'])),
                'SourceTransactionId': invoice['Id'],
                'Type': 'Increase',
                'ReasonCode': 'Standard Adjustment',
                'AccountingCode': 'Customer Cash On Account'
                }]
                zuora.create_object('CreditBalanceAdjustment', payload)

        self.log.info("Negative invoices adjusted. Job complete!")