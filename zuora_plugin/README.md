# Plugin - Zuora

This plugin provides an interface to the Zuora [REST API](https://www.zuora.com/developer/api-reference/) via the `zuora_restful_python` [package](https://github.com/bolaurent/zuora_restful_python) or to the Zuora [SOAP API](https://knowledgecenter.zuora.com/DC_Developers/G_SOAP_API) via the `zeep` [package](https://python-zeep.readthedocs.io/en/master/).

## Hooks
### ZuoraRestHook
This hook handles the authentication and request to Zuora via the REST API.

### ZuoraSOAPHook
This hook handles the authentication and request to Zuora via the SOAP API.

## Operators
### ZuoraToRedshiftOperator
This operator runs ZOQL in Zuora's backed via the REST API or SOAP API and then upserts the results to Redshift. The parameters it can accept include the following:

- `zuora_query`   		ZOQL query to submit to Zuora API.
- `target_table`        Table to upsert to in Redshift target.
- `primary_key`     	Primary key to use for upsert function.
- `field_list`        	List of fields to restrict upsert to. These MUST be in the same order as they appear in the table since the upsert function will
						use the structure of the current table to decide which fields and what order they must be inserted.
- `redshift_conn_id`	The Airflow connection ID for the Redshift database.
- `zuora_conn_id`       The Airflow connection ID for Zuora.
- `use_rest_api`  		Boolean to define whether to use the REST API (true) or SOAP API (false).
- `zuora_soap_wsdl`     Absolute path to the Zuora WSDL.
- `autocommit`     		Option to make sure the database commits the transactions submitted automatically.