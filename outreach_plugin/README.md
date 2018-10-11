# Plugin - Outreach.io

This plugin provides an interface to the Outreach.io [REST API](https://api.outreach.io/api/v2/docs)

## Hooks
### OutreachHook
This hook handles the authentication and request to Outreach. It can be used to call several methods from the API.

## Operators
### OutreachToRedshiftOperator
This operator will pull data from a resource and push it back to a Redshift table. The parameters it can accept include the following:

- `resource`   			Reference to Outreach resource to retrieve data from.
- `target_table`        Table to upsert records to.
- `primary_key`     	Primary key to use during upsert operation.
- `ordered_field_list`	Ordered dict of fields to use for upsert. These fields must be in the same order as they appear in the target table.
- `filter`				Boolean to tell whether to filter the query call to Outreach.
- `filter_field`		Field on which to apply query filter.
- `filter_statement`	Tells how query call should filter based on the API documentation.
- `page_limit`			Limit for rows returned to 1 page.
- `page_offset`			Which page to return.
- `sort`				Boolean to tell whether to sort query call.
- `sort_statement`		Tells how query call should sort results.
- `aws_conn_id`			Reference to connection to AWS.
- `outreach_conn_id`	Reference to connection to Outreach.io.
- `database_conn_id` 	Reference to target Redshift cluster.