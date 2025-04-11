import pendulum
from airflow.decorators import dag
from helpers.check_files.checks import callback_calc_hydra
from ecmwf_dev.tasks.ecmwf_as_00.collect import collect_group
from ecmwf_dev.tasks.ecmwfe46_glo.clients import clients
from ecmwf_dev.tasks.ecmwfe46_glo.hydra import hydra
from helpers.check_files.checks import callback_dataset
from helpers.common import task_failure_alert
from notifiers.notifier import Notifier

emails = [
    #"christie.souza@climatempo.com.br",
    #"marcos.pereira@climatempo.com.br",
    "lucas.marten@climatempo.com.br",
]

configs = {
    "dag_id": "ecmwfe46_glo_dev",
    "default_args": {
        "owner": "airflow",
        #"retries": 4,
        #"retry_delay": pendulum.duration(minutes=1),
        "on_success_callback": callback_dataset,
        "on_failure_callback": task_failure_alert,
        "on_execute_callback": callback_calc_hydra,
        "target": "ecmwfe46_glo",
        "queue": "master",
        "email": emails,
        "on_failure_callback": Notifier(state="retry", emails=emails),
        "email_on_failure": True,
    },
    "schedule_interval": "10 0 * * *",
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "tags": ["master", "ecmwf", "forecast"],
    "doc_md": """[Github] """,
    "description": "description",
}


@dag(**configs)
def ecmwfe46_glo_dev():
    date_string = "data_interval_start"
    run_hour = "00"
    output_dir = f"/data/seasonal/ecmwfe46_glo/RAW/%Y/%j/{run_hour}/"

    collect = collect_group(
        date_string=date_string,
        run_hour=run_hour,
        output_dir=output_dir,
        prefixes=["D3F"],
    ) 

    collect >> hydra(date_string, run_hour) >> clients(date_string, run_hour)

ecmwfe46_glo_dev()
