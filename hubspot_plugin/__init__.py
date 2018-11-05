from airflow.plugins_manager import AirflowPlugin

from hubspot_plugin.hooks.hubspot_hook import HubspotHook
from hubspot_plugin.operators.postgres_to_hubspot_operator import PostgresToHubspotOperator

class HubspotPlugin(AirflowPlugin):
    name = "hubspot_plugin"
    operators = [PostgresToHubspotOperator]
    hooks = [HubspotHook]
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []