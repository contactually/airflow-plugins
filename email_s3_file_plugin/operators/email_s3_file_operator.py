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
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

# Airflow Base Classes
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.email import send_MIME_email, get_email_address_list
from airflow import configuration
from email.utils import formatdate

# Airflow Extended Classes 
from airflow.hooks.S3_hook import S3Hook


class EmailS3FileOperator(BaseOperator):
    """
    Reads a .sql file from S3 and executes it in Redshift
    :param filename: name of file to email
    :type filename: string
    :param to: list of emails to send the email to. (templated)
    :type to: list or string (comma or semicolon delimited)
    :param subject: subject line for the email. (templated)
    :type subject: string
    :param html_content: content of the email, html markup
        is allowed. (templated)
    :type html_content: string
    :param s3_bucket: reference to a specific S3 bucket where .sql file is stored
    :type s3_bucket: string
    :param s3_key: reference to a specific S3 key identifying .sql file
    :type s3_key: string
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: string
    :param cc: list of recipients to be added in CC field
    :type cc: list or string (comma or semicolon delimited)
    :param bcc: list of recipients to be added in BCC field
    :type bcc: list or string (comma or semicolon delimited)
    :param attachment_extension: suffix for filename. e.g. .csv, .txt, etc.
    :param attachment_extension: string
    :param mime_subtype: MIME sub content type
    :type mime_subtype: string
    :param mime_charset: character set parameter added to the Content-Type
        header.
    :type mime_charset: string
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            filename,
            to,
            subject,
            html_content,
            s3_bucket,
            s3_key,
            aws_conn_id,
            cc=None,
            bcc=None,
            attachment_extension='.csv',
            mime_subtype='mixed',
            mime_charset='ascii',
            *args, **kwargs):
        super(EmailS3FileOperator, self).__init__(*args, **kwargs)
        self.filename = filename
        self.to = to
        self.subject = subject
        self.html_content = html_content
        self.cc = cc
        self.bcc = bcc
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_id = aws_conn_id
        self.attachment_extension = attachment_extension
        self.mime_subtype = mime_subtype
        self.mime_charset = mime_charset

    def execute(self, context):
        s3_hook = S3Hook('S3AirflowConn')
        file_list = s3_hook.list_keys(
            bucket_name=self.s3_bucket,
            prefix=self.s3_key,
            delimiter='/'
            )
        
        header = ''
        body = ''
        for file in file_list:
            if 'header' in file:
                header = s3_hook.read_key(
                    key=file,
                    bucket_name=self.s3_bucket
                    )
            else:
                body_text = s3_hook.read_key(
                    key=file,
                    bucket_name=self.s3_bucket
                    )
                body = body + body_text
        if header == '':
            contents = body
        else:
            contents = header + body

        SMTP_MAIL_FROM = configuration.conf.get('smtp', 'SMTP_MAIL_FROM')

        to = get_email_address_list(self.to)

        msg = MIMEMultipart(self.mime_subtype)
        msg['Subject'] = self.subject
        msg['From'] = SMTP_MAIL_FROM
        msg['To'] = ", ".join(self.to)
        recipients = self.to
        if self.cc:
            cc = get_email_address_list(self.cc)
            msg['CC'] = ", ".join(cc)
            recipients = recipients + cc

        if self.bcc:
            # don't add bcc in header
            bcc = get_email_address_list(self.bcc)
            recipients = recipients + bcc

        msg['Date'] = formatdate(localtime=True)
        mime_text = MIMEText(self.html_content, 'html', self.mime_charset)
        msg.attach(mime_text)

        filename = '{filename}{attachment_extension}'.format(filename=self.filename, attachment_extension=self.attachment_extension)
        attachment = MIMEText(contents, 'plain')
        attachment.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(attachment)

        send_MIME_email(SMTP_MAIL_FROM, recipients, msg)