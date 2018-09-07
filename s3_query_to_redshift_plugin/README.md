# Plugin - S3QueryToRedshift

This plugin retrieves provides an interface between S3 SQL scripts and Redshift.

## Hooks
### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

### PostgresHook
[Core Airflow PostgresHook](https://pythonhosted.org/airflow/_modules/postgres_hook.html).

## Operators
### S3QueryToRedshiftOperator
This operator retrieves SQL from S3 and runs the code on a specified Redshift DB. The parameters it can accept include the following:

- `s3_bucket`     		The S3 bucket where the .sql file is stored.
- `s3_key`        		The S3 key where the .sql file is stored.
- `process_name`  		The name of the process to pass to the logs.
- `redshift_conn_id`    The Airflow connection ID for the Redshift DB.
- `aws_conn_id`         The Airflow connection ID for AWS.
- `autocommit`        	Passes boolean to tell Redshift to autocommit after running the query.