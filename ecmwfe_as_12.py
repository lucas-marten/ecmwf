import pendulum
from airflow.decorators import dag
from ecmwf_dev.tasks.ecmwf_as_00.collect import collect_group
from helpers.check_files.checks import callback_dataset
from helpers.common import task_failure_alert
from notifiers.notifier import Notifier

emails = [
    "christie.souza@climatempo.com.br",
    "marcos.pereira@climatempo.com.br",
    "lucas.marten@climatempo.com.br",
]

configs = {
    "dag_id": "ecmwfe_as_12_dev_taskflow",
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
    "doc_md": """ECMWF AS 12""",
    "description": "description",
}


@dag(**configs)
def ecmwfe_as_12_dev_taskflow():

    hour = "12"
    date_str = f"{{{{ data_interval_end.strftime('%Y%m%d{hour}') }}}}"
    output_dir = "/data/forecast/ecmwfe_as/RAW/%Y/%j/12/"

    collect = collect_group(hour, output_dir, prefixes=["CLE"])

    collect


ecmwfe_as_12_dev_taskflow()
