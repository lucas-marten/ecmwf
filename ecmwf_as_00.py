import pendulum
from airflow.decorators import dag
from ecmwf_dev.tasks.ecmwf_as_00.collect import collect_group
from ecmwf_dev.tasks.ecmwf_as_00.hydra import (hydra_group,
                                               hydra_priority_group,
                                               k_tt_sweat_indexes)
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
    "dag_id": "ecmwf_as_00_dev_taskflow",
    "default_args": {
        "owner": "airflow",
        # "retries": 4,
        # "retry_delay": pendulum.duration(minutes=1),
        # "on_failure_callback": task_failure_alert,
        # "on_success_callback": callback_dataset,
        "target": "ecmwf_as",
        "queue": "master",
        # "email": emails,
        # "on_retry_callback": Notifier(state="retry", emails=emails),
        # "sla_miss_callback": Notifier(state="failure", emails=emails),
        # "email_on_failure": True,
        # "email_on_retry": True,
        # "email_on_sla_miss": True,
    },
    "schedule_interval": "30 05 * * *",
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "tags": ["master", "ecmwf", "forecast"],
    "doc_md": """ECMWF AS 00""",
    "description": "description",
}


@dag(**configs)
def ecmwf_as_00_dev_taskflow():

    hour = "00"
    date_str = f"{{{{ data_interval_end.strftime('%Y%m%d{hour}') }}}}"
    output_dir = "/data/forecast/ecmwf_as/RAW/%Y/%j/00/"

    collect = collect_group(hour, output_dir, prefixes=["S7S", "S7D"])
    hydra_priority = hydra_priority_group(date_str)
    k_tt_sweat = k_tt_sweat_indexes(date_str)
    hydra = hydra_group(date_str)
    sema = sema_group(date_str)

    collect >> hydra_priority >> [k_tt_sweat, hydra, sema]


ecmwf_as_00_dev_taskflow()
