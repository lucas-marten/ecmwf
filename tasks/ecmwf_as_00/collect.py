from airflow.decorators import task, task_group
from ecmwf_dev.tasks.ecmwf_as_00.utils import (check_download, check_s3,
                                               ct_datahub_decompression)


@task_group(group_id="collect_group", tooltip="collect_group")
def collect_group(hour, output_dir, prefixes=["S7D", "S7S"]):
    for prefix in prefixes:

        @task(task_id=f"check_s3_{prefix}")
        def task_check_s3(prefix, **context):
            return check_s3(
                prefix=prefix,
                hour=hour,
                output_dir=output_dir,
                **context,
            )

        @task(task_id=f"download_decompression_{prefix}")
        def task_download_decompression(prefix, **context):
            return ct_datahub_decompression(
                prefix=prefix,
                hour=hour,
                output_dir=output_dir,
                **context,
            )

        @task(task_id=f"check_download_{prefix}")
        def task_check_download(prefix, **context):
            return check_download(
                prefix=prefix,
                hour=hour,
                output_dir=output_dir,
                **context,
            )

        (
            task_check_s3(prefix)
            >> task_download_decompression(prefix)
            >> task_check_download(prefix)
        )
