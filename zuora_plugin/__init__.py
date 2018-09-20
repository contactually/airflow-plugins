from airflow.plugins_manager import AirflowPlugin

from zuora_plugin.hooks.zuora_rest_hook import ZuoraRestHook
from zuora_plugin.hooks.zuora_soap_hook import ZuoraSoapHook
from zuora_plugin.operators.zuora_to_redshift_operator import ZuoraToRedshiftOperator
from zuora_plugin.operators.zuora_bill_run_operator import ZuoraBillRunOperator

class ZuoraPlugin(AirflowPlugin):
    name = "zuora_plugin"
    operators = [ZuoraToRedshiftOperator, ZuoraBillRunOperator]
    hooks = [ZuoraRestHook, ZuoraSoapHook]
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []