import sys

import pendulum
from airflow.decorators import dag

sys.path.insert(1, "/airflow/dags")

from ecmwf_dev.tasks.ecmwf_as_00.collect import collect_group
from ecmwf_dev.tasks.ecmwf_as_00.hydra import hydra_group, hydra_priority_group
from ecmwf_dev.tasks.ecmwf_as_00.pos_check import task_check_dataset
from ecmwf_dev.tasks.ecmwf_as_00.sema import sema_group
from helpers.check_files.checks import callback_dataset
from helpers.common import task_failure_alert
from notifiers.notifier import Notifier

emails = [
    "christie.souza@climatempo.com.br",
    "marcos.pereira@climatempo.com.br",
    "lucas.marten@climatempo.com.br",
]

configs = {
    "dag_id": "ecmwf_as_00",
    "default_args": {
        "owner": "airflow",
        "retries": 4,
        "retry_delay": pendulum.duration(minutes=1),
        "on_failure_callback": task_failure_alert,
        "on_success_callback": callback_dataset,
        "target": "ecmwf_as",
        "queue": "master",
        "email": emails,
        "on_retry_callback": Notifier(state="retry", emails=emails),
        "sla_miss_callback": Notifier(state="failure", emails=emails),
        "email_on_failure": True,
        "email_on_retry": True,
        "email_on_sla_miss": True,
    },
    "schedule_interval": "30 05 * * *",
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "tags": ["master", "ecmwf", "forecast"],
    "doc_md": """[Github]""",
    "description": "description",
}


@dag(**configs)
def ecmwf_as_00():

    date_string = "data_interval_end"
    run_hour = "00"

    collect = collect_group(
        date_string=date_string,
        run_hour=run_hour,
        output_dir=f"/data/forecast/ecmwf_as/RAW/%Y/%j/{run_hour}/",
        prefixes=["S7S", "S7D"],
    )

    hydra_priority = hydra_priority_group(date_string=date_string, run_hour=run_hour)

    hydra = hydra_group(date_string=date_string, run_hour=run_hour)

    sema = sema_group(date_string=date_string)

    pos_check = task_check_dataset(
        date_string=date_string,
        run_hour=run_hour,
        path="/data/forecast/ecmwf_as/",
        model="ecmwf_as",
    )

    collect >> hydra_priority >> sema
    collect >> hydra_priority >> hydra >> pos_check


ecmwf_as_00()
