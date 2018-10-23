# Plugin - GoToWebinar

This plugin provides an interface to the Outreach.io [REST API](https://goto-developer.logmeininc.com/content/gotowebinar-api-reference-v2)

## Hooks
### GoToWebinarHook
This hook handles the authentication and request to GoToWebinar. It can be used to call several methods from the API.

## Operators
### GoToWebinarToRedshiftOperator
This operator will pull data from GoToWebinar and push it into the schema in Redshift. The parameters it can accept include the following:

- `from_time`   		Required start of datetime range in ISO8601 UTC format, e.g. 2015-07-13T10:00:00Z.
- `to_time`        		Required end of datetime range in ISO8601 UTC format, e.g. 2015-07-13T10:00:00Z.
- `aws_conn_id`			Reference to connection to AWS.
- `gotowebinar_conn_id`	Reference to connection to GoToWebinar.
- `database_conn_id` 	Reference to target Redshift cluster.