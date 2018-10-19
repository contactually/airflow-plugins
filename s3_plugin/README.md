# Plugin - S3

This plugin provides several interfaces to S3 via various hooks.

## Hooks
### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

### PostgresHook
[Core Airflow PostgresHook](https://pythonhosted.org/airflow/_modules/postgres_hook.html).

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
- `attachment_extension`Suffix for filename. e.g. .csv, .txt, etc.
- `mime_subtype`		MIME subtype.
- `mime_chartype`		Character set parameter added to the Content-Type header.

### CustomRedshiftToS3Operator
This operator retrieves SQL from S3, runs the code on a specified Redshift DB and then returns the result to S3 as a delimited text file. The parameters it can accept include the following:

- `query_s3_bucket`     The S3 bucket where the .sql file is stored.
- `query_s3_key`        The S3 key where the .sql file is stored.
- `dest_s3_bucket`      The S3 bucket where the result file is stored.
- `dest_s3_key`        	The S3 key where the result file is stored.
- `redshift_conn_id`    The Airflow connection ID for the Redshift DB.
- `aws_conn_id`         The Airflow connection ID for AWS.
- `unload_options`		Passes options specified by the UNLOAD command for AWS Redshift.
- `autocommit`        	Passes boolean to tell Redshift to autocommit after running the query.
- `parameters`			Passes custom parameters to psycopg library
- `headers`				Passes a list of strings to be added to query results; must be in correct order
						as results in SELECT statement in order to be appended correctly to result file.

### S3ToRedshiftOperator
This operator is similar to the one found in Airflow but functions for the S3 hook for Airflow v1.9. The parameters it can accept include the following:
- `schema`				Reference to a specific schema in Redshift DB.
- `table`				Reference to a specific table in Redshift DB.
- `s3_bucket`			The S3 bucket where the text file is to be COPY'd from.
- `s3_key`				The S3 key where the text file is to be COPY'd from.
- `redshift_conn_id`	The Airflow connection ID for the Redshift DB.
- `aws_conn_id`			The Airflow connection ID for AWS.
- `verify`				Whether or not to verify SSL certificates for S3 connection.
- `copy_options`		Reference to a list of COPY options
- `autocommit`			Specifies whether DML transactions are committed upon submission.
- `parameters`			Specifies parameters to be passed through to psycopg.

### S3QueryToRedshiftOperator
This operator retrieves SQL from S3 and runs the code on a specified Redshift DB. The parameters it can accept include the following:

- `s3_bucket`     		The S3 bucket where the .sql file is stored.
- `s3_key`        		The S3 key where the .sql file is stored.
- `process_name`  		The name of the process to pass to the logs.
- `redshift_conn_id`    The Airflow connection ID for the Redshift DB.
- `aws_conn_id`         The Airflow connection ID for AWS.
- `autocommit`        	Passes boolean to tell Redshift to autocommit after running the query.

###S3QueryToLambdaOperator
This operator reads SQL from S3, runs the code on a specified Redshift DB, submits the query results to a Lambda function, and converts the response to a csv stored back in a specified S3 location. The Lambda function must return a list of dictionaries in order to parse the response into a csv format. The parameters it can accept include the following:

- `query_s3_bucket`     The S3 bucket where the .sql file is stored.
- `query_s3_key`        The S3 key where the .sql file is stored.
- `dest_s3_bucket`      The S3 bucket where the result file is stored.
- `dest_s3_key`        	The S3 key where the result file is stored.
- `function_name`		AWS Lambda function name.
- `aws_region`			AWS region.
- `redshift_conn_id`	The Airflow connection ID for the Redshift DB.
- `aws_conn_id`			The Airflow connection ID for AWS.

###UpsertS3FileToRedshiftOperator
This operator reads a delimited file from S3 and upserts the contents to a Redshift table. The headers must appear in order in the first row of the delimited file. The parameters it can accept include the following:

- `s3_bucket`     		The S3 bucket where the delimited file is stored.
- `s3_key`        		The S3 key where the delimited file is stored.
- `file_delimiter`      File delimiter.
- `target_table`        Target table to upsert data to.
- `primary_key`			Primary key to use in upsert operation.
- `redshift_conn_id`	The Airflow connection ID for the Redshift DB.
- `aws_conn_id`			The Airflow connection ID for AWS.
- `aws_region`			AWS region.
- `autocommit`        	Passes boolean to tell Redshift to autocommit after running the query.
