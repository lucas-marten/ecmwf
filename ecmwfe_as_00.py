import pendulum
from airflow.decorators import dag

from ecmwf_dev.tasks.ecmwf_as_00.collect import collect_group
from ecmwf_dev.tasks.ecmwf_as_00.pos_check import task_check_dataset

from ecmwf_dev.tasks.ecmwfe_as_00.hydra import hydra, hydra_priority

from helpers.check_files.checks import callback_dataset
from helpers.check_files.checks import callback_calc_hydra

from helpers.common import task_failure_alert
from notifiers.notifier import Notifier

emails = [
    #"christie.souza@climatempo.com.br",
    #"marcos.pereira@climatempo.com.br",
    "lucas.marten@climatempo.com.br",
]


configs = {
    "dag_id": "ecmwfe_as_00",
    "default_args": {
        "owner": "airflow",
        "retries": 4,
        "retry_delay": pendulum.duration(minutes=1),
        "on_success_callback": callback_dataset,
        "on_failure_callback": task_failure_alert,
        "on_execute_callback": callback_calc_hydra,
        "target": "ecmwfe_as",
        "queue": "master",
        "email": emails,
        "on_failure_callback": Notifier(state="failure", emails=emails),
        "email_on_failure": True,
    },
    "schedule_interval": "30 07 * * *",
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "tags": ["master", "ecmwf", "forecast"],
    "doc_md": """[Github] """,
    "description": "description",
}


@dag(**configs)
def ecmwfe_as_00():
    run_hour = "00"
    date_string = "data_interval_end"
    output_dir = f"/data/forecast/ecmwfe_as/RAW/%Y/%j/{run_hour}/"
    
    collect = collect_group(date_string, run_hour, output_dir, prefixes=['CLE'])
    _hydra = hydra(date_string, run_hour)
    _hydra_priority = hydra_priority(date_string, run_hour)
    pos_check = task_check_dataset(
        date_string=date_string,
        run_hour=run_hour,
        path="/data/forecast/ecmwfe_as/",
        model="ecmwfe_as",
    )
    collect >> _hydra_priority >> _hydra >> pos_check


ecmwfe_as_00()
