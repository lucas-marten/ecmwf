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
    "dag_id": "ecmwfe46_glo_dev_taskflow",
    "default_args": {
        "owner": "airflow",
        # "retries": 4,
        # "retry_delay": pendulum.duration(minutes=1),
        # "on_failure_callback": task_failure_alert,
        # "on_success_callback": callback_dataset,
        "target": "ecmwfe46_glo",
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
def ecmwfe46_glo_dev_taskflow():

    hour = "00"
    date_str = f"{{{{ data_interval_end.strftime('%Y%m%d{hour}') }}}}"
    output_dir = "/data/seasonal/ecmwfe46_glo/RAW/%Y/%j/00/"

    collect = collect_group(hour, output_dir, prefixes=["D3F"])
    # hydra_priority = hydra_priority_group(date_str)
    # k_tt_sweat = k_tt_sweat_indexes(date_str)
    # hydra = hydra_group(date_str)

    collect  # >> hydra_priority >> [k_tt_sweat, hydra]


ecmwfe46_glo_dev_taskflow()
