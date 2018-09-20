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

### ZuoraBillRunOperator
This operator creates a bill run according to the parameters passed. The parameters it can accept include the following:
- `zuora_conn_id`						Connection ID for Zuora
- `invoice_date`						Date of invoice in format 'YYYY-MM-DD'
- `target_date`							Target date in format 'YYYY-MM-DD'
- `execute_on_day`						Day to create bill run. Use '*' for every day, an integer for day of the month, or 'Last Day of Month'
- `execute_on_timezone`					Timezone value used to check whether to create bill run on that day
- `account_id`							Zuora account ID if bill run is for one account (optional)
- `auto_email`							Tells Zuora whether to automatically email invoices after bill run is posted
- `auto_post`							Tells Zuora whether to automatically post invoices after bill run is created
- `auto_renewal`						Tells Zuora whether to automatically renew subscription after bill run is created
- `batch`								Batch ID for Zuora bill run (required if account_id not passed)
- `bill_cycle_day`						Which bill cycle days to include in bill run
- `charge_type_to_exclude`				Which charge types to exclude from bill run
- `no_email_for_zero_amount_invoice`	Whether to email zero balance invoice customers