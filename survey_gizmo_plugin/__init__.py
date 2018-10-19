from airflow.plugins_manager import AirflowPlugin

from survey_gizmo_plugin.hooks.survey_gizmo_hook import SurveyGizmoHook

class SurveyGizmoPlugin(AirflowPlugin):
    name = "survey_gizmo_plugin"
    operators = []
    hooks = [SurveyGizmoHook]
    # Leave in for explicitness even if not using
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []