from airflow.plugins_manager import AirflowPlugin

from aws_plugin.hooks.redshift_hook import RedshiftHook

class AwsPlugin(AirflowPlugin):
    name = "aws_plugin"
    operators = []
    hooks = [RedshiftHook]
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []