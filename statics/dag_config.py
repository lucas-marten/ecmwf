import pendulum

from helpers.check_files.checks import callback_dataset
from helpers.common import task_failure_alert
from notifiers.notifier import Notifier

emails = [
    "christie.souza@climatempo.com.br",
    "marcos.pereira@climatempo.com.br",
    "lucas.marten@climatempo.com.br",
]

configs = {
    "dag_id": "ecmwf_as_00_dev",
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
    "doc_md": """ECMWF AS 00""",
    "start_date": pendulum.today("UTC").add(days=-1),
}
