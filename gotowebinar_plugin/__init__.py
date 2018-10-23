from airflow.plugins_manager import AirflowPlugin

from gotowebinar_plugin.hooks.gotowebinar_hook import GoToWebinarHook
from gotowebinar_plugin.operators.gotowebinar_to_redshift_operator import GoToWebinarToRedshiftOperator

class GoToWebinarPlugin(AirflowPlugin):
    name = "gotowebinar_plugin"
    operators = [GoToWebinarToRedshiftOperator]
    hooks = [GoToWebinarHook]
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []