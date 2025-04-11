import sys

import pendulum
from airflow.decorators import dag, task, task_group

sys.path.insert(1, "/airflow/dags")
from ecmwf_dev.tasks.ecmwfai_as.collect import collect
from ecmwf_dev.tasks.ecmwfai_as.hydra import hydra
from ecmwf_dev.tasks.ecmwf_as_00.pos_check import task_check_dataset

from helpers.check_files.checks import callback_dataset
from helpers.common import task_failure_alert
from notifiers.notifier import Notifier

emails = [
    #"christie.souza@climatempo.com.br",
    #"marcos.pereira@climatempo.com.br",
    "lucas.marten@climatempo.com.br",
]

configs = {
    "dag_id": "ecmwfai_as_dev",
    "default_args": {
        "owner": "airflow",
        #"retries": 4,
        #"retry_delay": pendulum.duration(minutes=1),
        #"on_failure_callback": task_failure_alert,
        #"on_success_callback": callback_dataset,
        "target": "ecmwfai_as",
        "queue": "master",
        #"email": emails,
        #"on_retry_callback": Notifier(state="retry", emails=emails),
        #"sla_miss_callback": Notifier(state="failure", emails=emails),
        #"email_on_failure": True,
        #"email_on_retry": True,
        #"email_on_sla_miss": True,
    },
    "schedule_interval": "30 08 * * *",
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "tags": ["master", "ecmwf", "forecast"],
    "doc_md": """[Github]""",
    "description": "description",
}


@dag(**configs)
def ecmwfai_as_dev():

    date_string = "data_interval_end"
    run_hour = "00"
    pos_check = task_check_dataset(
        date_string=date_string,
        run_hour=run_hour,
        path="/data/forecast/ecmwfai_as/",
        model="ecmwfai_as",
    )
    collect(date_string, run_hour) >> hydra(date_string, run_hour) >> pos_check


ecmwfai_as_dev()
