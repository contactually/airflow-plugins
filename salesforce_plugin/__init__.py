from airflow.plugins_manager import AirflowPlugin

from salesforce_plugin.hooks.salesforce_hook import SalesforceHook
from salesforce_plugin.operators.salesforce_upsert_operator import SalesforceUpsertOperator

class SalesforcePlugin(AirflowPlugin):
    name = "SalesforcePlugin"
    operators = [SalesforceUpsertOperator]
    hooks = [SalesforceHook]
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []