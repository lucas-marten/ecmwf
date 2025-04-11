import sys

import pendulum
from airflow.decorators import dag, task, task_group

sys.path.insert(1, "/airflow/dags")
from ecmwf_dev.tasks.ecmwf_fct.collect import collect
from ecmwf_dev.tasks.ecmwf_fct.hydra import hydra
from ecmwf_dev.tasks.ecmwf_as_00.pos_check import task_check_dataset


from helpers.check_files.checks import callback_dataset, callback_calc_hydra
from helpers.common import task_failure_alert
from notifiers.notifier import Notifier

emails = [
    #"christie.souza@climatempo.com.br",
    #"marcos.pereira@climatempo.com.br",
    "lucas.marten@climatempo.com.br",
]

configs = {
    "dag_id": "ecmwf_fct",
    "default_args": {
        "owner": "airflow",
        "retries": 4,
        "retry_delay": pendulum.duration(minutes=1),
        "on_failure_callback": task_failure_alert,
        "on_success_callback": callback_dataset,
        "on_execute_callback": callback_calc_hydra,
        "target": "ecmwf_fct",
        "queue": "master",
        "email": emails,
        #"on_retry_callback": Notifier(state="retry", emails=emails),
        #"sla_miss_callback": Notifier(state="failure", emails=emails),
        #"email_on_failure": True,
        #"email_on_retry": True,
        #"email_on_sla_miss": True,
    },
    "schedule_interval": "00 21 * * 1,4",
    "start_date": pendulum.today("UTC").add(days=-14),
    "catchup": False,
    "concurrency": 2,
    "tags": ["master", "ecmwf", "forecast"],
    "doc_md": """[Github]""",
    "description": "description",
}


@dag(**configs)
def ecmwf_fct():

    date_string = "data_interval_end"

    pos_check = task_check_dataset(
        date_string=date_string,
        run_hour='00',
        path="/data/forecast/ecmwf_fct/",
        model="ecmwf_fct",
    )

    collect(date_string) >> hydra(date_string) >> pos_check

ecmwf_fct()
