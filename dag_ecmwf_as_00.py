from datetime import datetime

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator


@dag(
    dag_id="ecmwf_as_00_dev",
    default_args={
        "owner": "airflow",
        "retries": 14,
        "retry_delay": pendulum.duration(minutes=1),
        "queue": "master",
    },
    schedule="30 05 * * *",
    start_date=pendulum.today("UTC").add(days=-1),
    catchup=False,
    tags=["master", "ecmwf", "forecast"],
)
def ecmwf_as_00_dev():
    """
    ## DOCS
    """

    def clear_raw_files(outputfiles, prefix):
        import os

        for f in os.listdir(outputfiles):
            if f.startswith(prefix):
                path_rm = os.path.join(outputfiles, f)
                if os.path.isfile(path_rm):
                    os.remove(path_rm)

        return True

    def decompress(output_dir, prefix):
        import bz2
        import os
        from multiprocessing.dummy import Pool

        def _decompress(file):
            with open(file, "rb") as source, open(
                file.replace(".bz2", ""), "wb"
            ) as dest:
                dest.write(bz2.decompress(source.read()))
                if os.path.isfile(file):
                    os.remove(file)

        files = [
            os.path.join(output_dir, f)
            for f in os.listdir(output_dir)
            if f.startswith(prefix)
        ]
        with Pool(processes=10) as pool:
            pool.map(_decompress, files)

        return True

    
    @task_group
    def collect():

        @task
        def check(prefix, **context):
            airflow_date = "data_interval_end"
            script_check = "python3 /airflow-dev/tools/ct-datahub/datahub/aws/check.py"
            output_dir = "/data/forecast/ecmwf_as/RAW/%Y/%j/00/"
            cmd = datetime.fromisoformat(str(context[airflow_date])).strftime(
                f"{script_check} -pf {prefix}%m%d0000 -bn ecmwf-upload -of {output_dir} -pn stormgeo --date %Y%m%dT0000"
            )
            task = BashOperator(task_id=f"check_{prefix}", bash_command=cmd)
            task.execute(context=context)
            return True

        @task
        def download_and_decompress(prefix, **context):
            import os

            date = datetime.fromisoformat(str(context["data_interval_end"])).replace(
                tzinfo=None
            )
            script = "python3 /airflow-dev/tools/ct-datahub/datahub/aws/download.py"
            outputfiles = date.strftime("/data/forecast/ecmwf_as/RAW/%Y/%j/00/")

            clear_raw_files(outputfiles, prefix)

            cmd = date.strftime(
                f"{script} -pf {prefix}%m%d0000 -bn ecmwf-upload -of {outputfiles} --download_all -pn stormgeo --date %Y%m%dT0000"
            )
            task = BashOperator(task_id=prefix, bash_command=cmd)
            task.execute(context=context)

            files = os.listdir(outputfiles)

            if prefix == "S7D" and len(files) >= 60:
                decompress(outputfiles, prefix)
            elif prefix == "S7S" and len(files) >= 73:
                decompress(outputfiles, prefix)
            else:
                raise AirflowException("ct_datahub(): Arquivos incompletos")

            return True

        for prefix in ['S7D', 'S7S']:
            check(prefix=prefix) >> download_and_decompress(prefix=prefix)
    collect()

ecmwf_as_00_dev()
