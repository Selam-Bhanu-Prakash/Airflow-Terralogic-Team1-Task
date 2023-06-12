from airflow import DAG
from datetime import datetime, timedelta
# from my_operators import IMAPPluginOperator, SpreadsheetPluginOperator, DrivePluginOperator
from my_operators import IMAPPluginOperator, SpreadsheetPluginOperator, DrivePluginOperator



default_args = {
    'start_date': datetime(2023, 6, 9),
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    schedule_interval= None
    # timedelta(minutes=2)
)



email_login = IMAPPluginOperator(
    task_id='email_login',
    dag=dag
)

spreadsheet_task = SpreadsheetPluginOperator(
    task_id='spreadsheet_task',
    keys = 'bhanu.json',
    dag=dag
)


drive_task = DrivePluginOperator(
    task_id='drive_task',
    dag=dag
)

email_login >> spreadsheet_task >> drive_task


