from airflow.plugins_manager import AirflowPlugin

from s3_query_to_redshift_plugin.operators.s3_query_to_redshift_operator import S3QueryToRedshiftOperator

class S3QueryToRedshiftPlugin(AirflowPlugin):
    name = "s3_query_to_redshift_plugin"
    operators = [S3QueryToRedshiftOperator]
    hooks = []
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []