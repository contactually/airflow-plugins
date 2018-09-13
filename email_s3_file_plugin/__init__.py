from airflow.plugins_manager import AirflowPlugin

from email_s3_file_plugin.operators.email_s3_file_operator import EmailS3FileOperator

class EmailS3FilePlugin(AirflowPlugin):
    name = "email_s3_file_plugin"
    operators = [EmailS3FileOperator]
    hooks = []
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []