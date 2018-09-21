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

# Airflow Base Classes
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Airflow Extended Classes 
from zuora_plugin.hooks.zuora_rest_hook import ZuoraRestHook


class ZuoraCancelDelinquentCustomerOperator(BaseOperator):
    """
    Determines delinquent customers and cancels their subscriptions. Also deals with credit balance and invoice adjustments as necessary
    :param zuora_conn_id: connection ID for Zuora
    :type zuora_conn_id: string
    :param target_date: target date in format 'YYYY-MM-DD'
    :type target_date: string
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            zuora_conn_id,
            target_date,
            *args, **kwargs):
        super(ZuoraCancelDelinquentCustomerOperator, self).__init__(*args, **kwargs)
        self.zuora_conn_id = zuora_conn_id
        self.target_date = target_date

    def execute(self, context):
        client = ZuoraRestHook(self.zuora_conn_id)

        delinquent_subscriptions = self.determine_delinquent_customers(client)
        for subscription in delinquent_subscriptions:
            cancel_date = self.determine_cancellation_date(subscription)
            client.cancel_subscription(subscription['id'], {"cancellationPolicy": "SpecificDate", "cancellationEffectiveDate": cancel_date, "invoiceCollect": True})
            open_invoices = self.determine_open_invoices(subscription, client)

            # Deal with open invoices
            for invoice in open_invoices:
                credit_balance = self.determine_credit_balance(subscription, client)
                invoice_balance = float(invoice['Balance'])

                # Adjust invoice down by adding to or subtracting from credit balance
                if invoice_balance < 0 or credit_balance > 0:
                    credit_balance_adj = self.calculate_credit_balance_adj(invoice_balance, credit_balance)
                    client.create_credit_balance_adjustment({"Amount": credit_balance_adj, 
                                                             "SourceTransactionId": invoice['Id'],
                                                             "Type": "Increase" if invoice_balance < 0 else "Decrease", 
                                                             "ReasonCode": "Cancel - Nonpayment", 
                                                             "AccountingCode": "Customer Cash On Account"
                                                            })

                # Recalculate invoice balance
                invoice_balance = self.invoice_balance(invoice, client)

                # Adjust invoice down with invoice adjustment
                if invoice_balance > 0:
                    client.create_object('invoice-adjustment', {"AccountingCode": "Financing Expenses:Bad Debt Expense", 
                                                                "Amount": invoice_balance, 
                                                                "InvoiceId": invoice['Id'],
                                                                "Type": "Credit",
                                                                "ReasonCode": "Write-off"
                                                               })
            # Close payment method after cancellation + adjustments
            payload = {"PaymentMethodStatus": "Closed"}
            client.update_object('payment-method', subscription['payment_method'], payload)

    def determine_delinquent_customers(self, client):
        query = """
        select Account.Id as "account", Subscription.Id as "id", DefaultPaymentMethod.Id as "payment_method", DefaultPaymentMethod.LastTransactionDateTime as "last_failed", Subscription.ContractEffectiveDate as "contract_effective_date"
        from Subscription
        where Status = 'Active' and DefaultPaymentMethod.NumConsecutiveFailures >= 4 and Account.CustomerType__c = 'SMB'
        """

        results = client.aqua_query(query)
        return results

    def determine_cancellation_date(self, subscription):
        last_failed = subscription['last_failed'][:10]
        cancel_date = max(last_failed, subscription['contract_effective_date'])
        return cancel_date

    def determine_open_invoices(self, subscription, client):
        query = """
        select Id, InvoiceNumber, Balance
        from Invoice
        where AccountId = '{account_id}' and Balance != 0 and Status = 'Posted'
        """.format(account_id=subscription['account'])

        open_invoices = client.query(query)
        return open_invoices

    def determine_credit_balance(self, subscription, client):
        query = """
        select CreditBalance
        from Account
        where Id = '{account}'
        """.format(account=subscription['account'])

        credit_balance = client.query(query)
        credit_balance = float(credit_balance[0]['CreditBalance'])
        return credit_balance

    def calculate_credit_balance_adj(self, invoice_balance, credit_balance):
        if invoice_balance < 0 or credit_balance > invoice_balance:
            return abs(invoice_balance)
        elif credit_balance <= invoice_balance:
            return abs(credit_balance)

    def invoice_balance(self, invoice, client):
        query = """
        select Balance
        from Invoice
        where Id = '{invoice_id}'
        """.format(invoice_id=invoice['Id'])

        invoice_balance = client.query(query)
        invoice_balance = float(invoice_balance[0]['Balance'])
        return invoice_balance