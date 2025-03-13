from airflow.decorators import task, task_group
from ecmwf_dev.helpers.utils import check_download
from ecmwf_dev.helpers.utils import check_s3
from ecmwf_dev.helpers.utils import ct_datahub_decompression


@task_group
def collect_group():
    for prefix in ["S7D", "S7S"]:
        @task(task_id=f"check_s3_{prefix}")
        def task_check_s3(prefix, **context):
            return check_s3(
                prefix=prefix,
                hour="00",
                output_dir="/data/forecast/ecmwf_as/RAW/%Y/%j/00/",
                **context,
            )

        @task(task_id=f"download_decompression_{prefix}")
        def task_download_decompression(prefix, **context):
            return ct_datahub_decompression(
                prefix=prefix,
                hour="00",
                output_dir="/data/forecast/ecmwf_as/RAW/%Y/%j/00/",
                **context,
            )

        @task(task_id=f"check_download_{prefix}")
        def task_check_download(prefix, **context):
            return check_download(
                prefix=prefix,
                hour="00",
                output_dir="/data/forecast/ecmwf_as/RAW/%Y/%j/00/",
                **context,
            )
        (task_check_s3(prefix) >> \
            task_download_decompression(prefix) >> \
                task_check_download(prefix))
