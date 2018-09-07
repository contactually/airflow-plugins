# Plugin - S3QueryToRedshift

This plugin retrieves extends capabilities for writing the results from custom SQL queries in Redshift to S3.

## Hooks
### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

### PostgresHook
[Core Airflow PostgresHook](https://pythonhosted.org/airflow/_modules/postgres_hook.html).

## Operators
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