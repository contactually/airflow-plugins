from airflow.plugins_manager import AirflowPlugin

from zoom_plugin.hooks.zoom_hook import ZoomHook
from zoom_plugin.operators.zoom_webinar_to_redshift_operator import ZoomWebinarToRedshiftOperator

class ZoomPlugin(AirflowPlugin):
    name = "zoom_plugin"
    operators = [ZoomWebinarToRedshiftOperator]
    hooks = [ZoomHook]
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []