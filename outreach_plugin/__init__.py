from airflow.plugins_manager import AirflowPlugin

from outreach_plugin.hooks.outreach_hook import OutreachHook
from outreach_plugin.operators.outreach_to_redshift_operator import OutreachToRedshiftOperator

class OutreachPlugin(AirflowPlugin):
    name = "outreach_plugin"
    operators = [OutreachToRedshiftOperator]
    hooks = [OutreachHook]
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []