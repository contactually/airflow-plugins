from airflow.plugins_manager import AirflowPlugin

from youcanbookme_plugin.hooks.youcanbookme_hook import YoucanbookmeHook

class YoucanbookmePlugin(AirflowPlugin):
    name = "youcanbookme_plugin"
    operators = []
    hooks = [YoucanbookmeHook]
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []