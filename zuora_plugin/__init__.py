from airflow.plugins_manager import AirflowPlugin

from zuora_plugin.hooks.zuora_rest_hook import ZuoraRestHook
from zuora_plugin.hooks.zuora_soap_hook import ZuoraSoapHook
from zuora_plugin.operators.zuora_to_redshift_operator import ZuoraToRedshiftOperator

def relative_file_path(relative_filename):
	import os
	dirname = os.getenv('AIRFLOW_HOME')
	filename = os.path.join(dirname, relative_filename)

	return filename

class ZuoraPlugin(AirflowPlugin):
    name = "zuora_plugin"
    operators = [ZuoraToRedshiftOperator]
    hooks = [ZuoraRestHook, ZuoraSoapHook]
    # Leave in for explicitness even if not using
    executors = []
    macros = [relative_file_path]
    admin_views = []
    flask_blueprints = []
    menu_links = []