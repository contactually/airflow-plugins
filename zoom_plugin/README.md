# Plugin - Zoom.us

This plugin provides an interface to the Zoom.us [REST API](https://zoom.github.io/api/#introduction)

## Hooks
### ZoomHook
This hook handles the authentication and request to Zoom. It can be used to call several methods from the API.

## Operators
### ZoomToRedshiftOperator
This operator will pull data from a resource and push it back to a Redshift table. The parameters it can accept include the following:

- `user_id`   			Reference to the user ID to retrieve data from that account.
- `schema`		        Schema to upsert records to.
- `target_table`     	Table to upsert records to.
- `field_list`			List of field column names. These fields must be in the same order as they appear in the target table.
- `aws_conn_id`			Reference to connection to AWS.
- `zoom_conn_id`		Reference to connection to Zoom.
- `database_conn_id` 	Reference to target Redshift cluster.