from airflow.decorators import task, task_group
from operators.Hydra import HydraOperator
from airflow.operators.bash import BashOperator

@task_group
def sema_group(date_str):
    cmd_extract = f"/airflow/dags/helpers/forecast/send_sema_extract.py  --category forecast --model ecmwf_as --date {date_str} --stackdir /airflow/dags/helpers/forecast/statics/sema_extract/ --type csvs"
    cmd_zip = f"/airflow/dags/helpers/forecast/send_sema_extract.py  --category forecast --model ecmwf_as --date {date_str} --stackdir /airflow/dags/helpers/forecast/statics/sema_extract/ --type zip"

    @task(task_id='extraction')
    def task_extraction():
        return BashOperator(task_id="extraction", bash_command=cmd_extract)
    
    @task(task_id="zip_files")
    def task_zip_files():
        return BashOperator(task_id="zip_files", bash_command=cmd_zip)
    
    @task(task_id='send_mail')
    def task_send_mail():
        return HydraOperator(
            task_id="send_mail",
            target="sema_extractions",
            process="send_mail",
            client="ecmwf10",
            date=date_str,
        )
    task_extraction() >> task_zip_files() >> task_send_mail() 