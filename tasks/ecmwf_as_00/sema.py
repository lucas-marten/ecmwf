from airflow.decorators import task, task_group


@task_group(group_id="sema_group", tooltip="sema_group")
def sema_group(date_string):
    from airflow.operators.bash import BashOperator

    from operators.Hydra import HydraOperator


    date_hydra = f"{{{{ {date_string}.strftime('%Y%m%d00') }}}}"
    date_sema = f"{{{{ {date_string}.strftime('%Y-%m-%dT00:00:00') }}}}"

    cmd_extract = f"/airflow/dags/helpers/forecast/send_sema_extract.py  --category forecast --model ecmwf_as --date {date_sema} --stackdir /airflow/dags/helpers/forecast/statics/sema_extract/ --type csvs"
    cmd_zip = f"/airflow/dags/helpers/forecast/send_sema_extract.py  --category forecast --model ecmwf_as --date {date_sema} --stackdir /airflow/dags/helpers/forecast/statics/sema_extract/ --type zip"

    extraction = BashOperator(task_id="extraction", bash_command=cmd_extract)

    zip_files = BashOperator(task_id="zip_files", bash_command=cmd_zip)

    send_mail = HydraOperator(
        task_id="send_mail",
        target="sema_extractions",
        process="send_mail",
        client="ecmwf10",
        date=date_hydra,
    )

    extraction >> zip_files >> send_mail
