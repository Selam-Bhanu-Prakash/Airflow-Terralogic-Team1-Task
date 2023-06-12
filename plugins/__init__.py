from airflow.plugins_manager import AirflowPlugin
from myplugins.my_operators import IMAPPluginOperator, SpreadsheetPluginOperator, DrivePluginOperator
class myplugins(AirflowPlugin):
    name ="my_operators"
    operators =[IMAPPluginOperator, SpreadsheetPluginOperator,DrivePluginOperator]