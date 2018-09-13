from airflow.plugins_manager import AirflowPlugin

from custom_redshift_to_s3_plugin.operators.custom_redshift_to_s3_operator import CustomRedshiftToS3Operator

class CustomRedshiftToS3Plugin(AirflowPlugin):
    name = "custom_redshift_to_s3_plugin"
    operators = [CustomRedshiftToS3Operator]
    hooks = []
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []