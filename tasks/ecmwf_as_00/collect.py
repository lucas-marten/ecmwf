from airflow.decorators import task, task_group


@task_group(group_id="collect_group", tooltip="collect_group")
def collect_group(date_string, run_hour, output_dir, prefixes=["S7D", "S7S"]):
    import os
    from datetime import datetime, timedelta
    from multiprocessing.dummy import Pool

    import botocore
    from airflow.exceptions import AirflowException
    from airflow.operators.bash import BashOperator
    from boto3.session import Session

    from ecmwf_dev.tasks.ecmwf_as_00.utils import (
        clear_raws,
        decompress,
        get_ec_times,
        get_payload,
    )

    def parse_date(date_string, run_hour, context):
        date = datetime.fromisoformat(str(context[date_string]))
        return date.replace(tzinfo=None, hour=int(run_hour))

    for prefix in prefixes:

        @task(
            task_id=f"check_s3_{prefix}", retries=24, retry_delay=timedelta(minutes=5)
        )
        def check_s3(date_string, run_hour, prefix, **context):

            def _check_s3(prefix, s3):
                try:
                    s3.Object("ecmwf-upload", prefix).load()
                except botocore.exceptions.ClientError as e:
                    if e.response["Error"]["Code"] == "404":
                        raise FileExistsError(
                            f"check_aws(): Sem arquivo na AWS, erro 404: {prefix}"
                        )
                else:
                    print(f"AWS_profile(): file AWS OK: {prefix}")
                return True

            s3 = Session(profile_name="stormgeo").resource("s3")

            date = parse_date(date_string, run_hour, context)

            times = get_ec_times(date, prefix)

            payload = get_payload(date, prefix, s3, times)

            with Pool(processes=2) as pool:
                pool.starmap(_check_s3, payload)

            return

        @task(
            task_id=f"download_decompression_{prefix}",
            retries=5,
            retry_delay=timedelta(minutes=5),
            sla=timedelta(minutes=25),
        )
        def download_decompression(
            date_string, run_hour, prefix, output_dir, **context
        ):

            date = parse_date(date_string, run_hour, context)

            script = "python3 /airflow-dev/tools/ct-datahub/datahub/aws/download.py"
            output_dir = date.strftime(output_dir)

            os.makedirs(os.path.dirname(output_dir), exist_ok=True)

            clear_raws(output_dir, prefix)

            cmd = date.strftime(
                f"{script} -pf {prefix}%m%d{run_hour}00 -bn ecmwf-upload -of {output_dir} --download_all -pn stormgeo --date %Y%m%dT%H00"
            )
            task = BashOperator(task_id=prefix, bash_command=cmd)
            task.execute(context=context)

            files = os.listdir(output_dir)
            print(len(files))

            if prefix == "S7D" and len(files) >= 60:
                decompress(output_dir, prefix)
            elif prefix == "S7S" and len(files) >= 73:
                decompress(output_dir, prefix)
            elif prefix == "CLE" and len(files) >= 85:
                decompress(output_dir, prefix)
            elif prefix == "D3F" and len(files) >= 47:
                decompress(output_dir, prefix)

            else:
                raise AirflowException("ct_datahub(): Arquivos incompletos")

            return

        @task(task_id=f"check_download_{prefix}", sla=timedelta(minutes=60))
        def check_download(date_string, run_hour, prefix, output_dir, **context):

            date = parse_date(date_string, run_hour, context)

            output_dir = date.strftime(output_dir)

            times = get_ec_times(date, prefix)
            for time in times:

                if prefix in ["S7S", "S7D", "CLE"]:
                    _prefix = date.strftime(f"{prefix}%m%d%H")
                    file_name = time.strftime(f"{_prefix}00%m%d%H001")
                    path_in = os.path.join(output_dir, file_name)

                elif prefix == "D3F":
                    _prefix = date.strftime(f"{prefix}%m%d")
                    file_name = time.strftime(f"{_prefix}0000%m%d____1")
                    path_in = os.path.join(output_dir, file_name)

                else:
                    raise KeyError("check_s3(): Prefixo inválido")

                if os.path.isfile(path_in):
                    print(f"check_data(): Arquivo ainda está em: {path_in}")
                else:
                    raise FileNotFoundError(
                        f"check_data(): Arquivo ainda não está em: {path_in}"
                    )
            return

        check_s3 = check_s3(date_string, run_hour, prefix)
        download_decompress = download_decompression(
            date_string, run_hour, prefix, output_dir
        )
        check_download = check_download(date_string, run_hour, prefix, output_dir)

        check_s3 >> download_decompress >> check_download
