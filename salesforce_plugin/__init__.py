from airflow.plugins_manager import AirflowPlugin

from salesforce_plugin.hooks.salesforce_hook import SalesforceHook
from salesforce_plugin.operators.salesforce_upsert_operator import SalesforceUpsertOperator
from salesforce_plugin.operators.salesforce_bulk_upsert_operator import SalesforceBulkUpsertOperator

class SalesforcePlugin(AirflowPlugin):
    name = "SalesforcePlugin"
    operators = [SalesforceUpsertOperator, SalesforceBulkUpsertOperator]
    hooks = [SalesforceHook]
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []