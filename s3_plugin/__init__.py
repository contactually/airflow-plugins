from airflow.plugins_manager import AirflowPlugin

from s3_plugin.operators.email_s3_file_operator import EmailS3FileOperator
from s3_plugin.operators.custom_redshift_to_s3_operator import CustomRedshiftToS3Operator
from s3_plugin.operators.s3_query_to_redshift_operator import S3QueryToRedshiftOperator
from s3_plugin.operators.s3_query_to_lambda_operator import S3QueryToLambdaOperator
from s3_plugin.operators.s3_to_redshift_operator import S3ToRedshiftTransfer

class EmailS3FilePlugin(AirflowPlugin):
    name = "s3_plugin"
    operators = [EmailS3FileOperator, CustomRedshiftToS3Operator, S3QueryToRedshiftOperator, S3QueryToLambdaOperator, S3ToRedshiftTransfer]
    hooks = []
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []