# Plugin - Hubspot

This plugin provides an interface to the Hubspot [REST API](https://developers.hubspot.com/docs/methods/)

## Hooks
### HubspotHook
This hook handles the authentication and request to Hubspot.

### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

### PostgresHook
[Core Airflow PostgresHook](https://pythonhosted.org/airflow/_modules/postgres_hook.html).

## Operators
### PostgresToHubspotOperator
This operator retrieves SQL from S3, runs the code on a specified Postgres DB, and then upserts the results to a Hubspot contact. The parameters it can accept include the following:

- `salesforce_object`   The Salesforce Object to be acted on. Possible values include:
							- Account
							- Contact
							- Opportunity
							- customObject__c
- `upsert_field`        The field name used as the key to match record for upsert operation.
- `query_s3_bucket`     The S3 bucket where the .sql file is stored.
- `query_s3_key`        The S3 key where the .sql file is stored.
- `database_conn_id`    The Airflow connection ID for the Postgres database.
- `aws_conn_id`         The Airflow connection ID for AWS.
- `salesforce_conn_id`  The Airflow connection ID for the Salesforce account.
- `no_null_list`        List of field names that should not be overwritten with blanks if query results in NULL for
						specified field. Ex. ['custom_field__c']
- `lookup_mapping`      Dictionary used to map lookup fields to respective external ID lookup. Ex. {'custom_field__r': 'external_field__c'}
						Note that lookup fields must be suffixed with '__r' in order to perform the lookup properly.
- `sql_params`			Allows for parameterization of SQL according to sqlalchemy docs;
        				e.g. 'WHERE id = :id' in SQL and pass {'id': 1} will parameterize :id as 1