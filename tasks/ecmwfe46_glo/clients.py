from airflow.decorators import dag, task_group, task

@task_group(group_id="clients", tooltip="clients")
def clients(date_string, run_hour):
    from airflow.operators.python import PythonOperator
    from operators.Hydra import HydraOperator
    from airflow.operators.bash import BashOperator
    import yaml
    from ecmwf_dev.tasks.ecmwf_as_00.utils import sheet_client

    date_hydra = f"{{{{ {date_string}.strftime('%Y%m%d{run_hour}') }}}}"

    with open(f"/airflow/dags/ecmwf_dev/statics/ecmwfe46_glo.yaml", "r") as file:
        data = yaml.safe_load(file)

    @task_group(group_id="extract", tooltip="extract")
    def extract(date_string, run_hour):
        for clients in data['ecmwfe46_glo']['extract']:
            task_extract = HydraOperator(
                task_id=f"extract_{clients}",
                process="extract",
                client=clients,
                date=date_hydra,
            )
            task_extract

    @task_group(group_id="sendmail", tooltip="sendmail")
    def sendmail(date_string, run_hour):
        for clients in data["ecmwfe46_glo"]["send"]:
            sendmail_group = HydraOperator(
                task_id=f"sendmail_{clients}",
                target="ecmwfe46",
                process="send_mail",
                client=clients,
                date=date_hydra,
            )
            sheet_group = PythonOperator(
                task_id=f"sheet_notify_{clients}",
                python_callable=sheet_client,
                op_kwargs={"files": ["ecmwfe46_glo"], "sheet": clients},
            )
            sendmail_group >> sheet_group

    @task_group(group_id="bucket", tooltip="bucket")
    def bucket(date_string, run_hour):
        extract_b = HydraOperator(
            task_id=f"extract_Echoenergia_ecmwfe46",
            process="extract",
            client="Echoenergia",
            date=date_hydra,
        )

        @task(task_id='send_bucket_Echoenergia_ecmwfe46')
        def send(client, model, date, cred, bucket, **context):
            import glob
            from datetime import datetime
            from google.cloud import storage
            
            date_ = datetime.strptime(date, "%Y%m%d%H").strftime("%Y/%j")
            for dire in glob.glob(f"/data/seasonal/{model}/CLIENTS/{client}/{date_}/*{date}.csv"):

                file = dire.split("/")[-1]
                file_bu = f"{client}/{model}/{date_}/{file}"
                gs_client = storage.Client.from_service_account_json(
                    f"/airflow/base/credentials/{cred}.json"
                )
                bucket = gs_client.bucket(file_bu)
                blob = bucket.blob(file_bu)
                print(file_bu)

                print(dire)
                print(file_bu)
                blob.upload_from_filename(dire, timeout=5 * 60)

            return

        extract_b >> \
            send('Echoenergia', 'ecmwfe46_glo', date_hydra, 'gcp-object', 'gcp-echoenergia-delivery')

    extract(date_string, run_hour) >> sendmail(date_string, run_hour) >> bucket(date_string, run_hour)