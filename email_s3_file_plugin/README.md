# Plugin - EmailS3File

This plugin uses the MIME email utils along with and S3 connection.

## Hooks
### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

## Operators
### EmailS3FileOperator
This operator sends a customized email from the configured SMTP backend and appends an attachment with all files in a given S3 bucket-key location:

- `filename`			The name of the attachment to be sent.
- `to`					List of email addresses to send the email to.
- `subject`				Subject of the email.
- `html_content`		Body of the email.
- `s3_bucket`     		The S3 bucket where the .sql file is stored.
- `s3_key`        		The S3 key where the .sql file is stored.
- `aws_conn_id`         The Airflow connection ID for AWS.
- `cc`					List of email addresses to CC.
- `bcc`					List of email addresses to BCC.
- `mime_subtype`		MIME subtype.
- `mime_chartype`		Character set parameter added to the Content-Type header.