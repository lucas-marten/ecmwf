from airflow.decorators import task_group

upload_variables = [
    "total_precipitation",
    "2m_air_temperature",
    "10m_wind_gust",
    "10m_wind_speed",
    "2m_relative_humidity",
    "10m_wind_direction",
    "sfc_pressure",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
]


@task_group
def send_layers():
    from airflow.operators.bash import BashOperator

    from operators.GCPBucket import GCPBucketUpload

    chname_tp = BashOperator(
        task_id=f"chname_tp",
        bash_command='{{ data_interval_end.strftime("cdo chname,total_precipitation,tp /data/forecast/ecmwf_as/total_precipitation/%Y/%j/ecmwf_as_total_precipitation_M000_%Y%m%d00.nc /data/forecast/ecmwf_as/total_precipitation/%Y/%j/ecmwf_as_total_precipitation_M000_%Y%m%d00.nc.chname") }}',
    )
    chname_rh = BashOperator(
        task_id=f"chname_rh",
        bash_command='{{ data_interval_end.strftime("cdo chname,2m_relative_humidity,r /data/forecast/ecmwf_as/2m_relative_humidity/%Y/%j/ecmwf_as_2m_relative_humidity_M000_%Y%m%d00.nc /data/forecast/ecmwf_as/2m_relative_humidity/%Y/%j/ecmwf_as_2m_relative_humidity_M000_%Y%m%d00.nc.chname") }}',
    )
    upload_tp = GCPBucketUpload(
        task_id=f"upload_tp",
        bucket_name="gcp-airflow-ingest",
        bucket_cred="gcp-object",
        template_path="/data/forecast/ecmwf_as/total_precipitation/%Y/%j/ecmwf_as_total_precipitation_M000_%Y%m%d00.nc.chname",
    )
    upload_rh = GCPBucketUpload(
        task_id=f"upload_rh",
        bucket_name="gcp-airflow-ingest",
        bucket_cred="gcp-object",
        template_path="/data/forecast/ecmwf_as/2m_relative_humidity/%Y/%j/ecmwf_as_2m_relative_humidity_M000_%Y%m%d00.nc.chname",
    )
    clear_chname_tp = BashOperator(
        task_id=f"clear_chname_tp",
        bash_command='{{ data_interval_end.strftime("rm /data/forecast/ecmwf_as/total_precipitation/%Y/%j/ecmwf_as_total_precipitation_M000_%Y%m%d00.nc.chname") }}',
    )
    clear_chname_rh = BashOperator(
        task_id=f"clear_chname_rh",
        bash_command='{{ data_interval_end.strftime("rm /data/forecast/ecmwf_as/2m_relative_humidity/%Y/%j/ecmwf_as_2m_relative_humidity_M000_%Y%m%d00.nc.chname") }}',
    )

    (
        chname_tp
        >> chname_rh
        >> upload_tp
        >> upload_rh
        >> clear_chname_tp
        >> clear_chname_rh
    )


@task_group
def upvars():
    from operators.GCPBucket import GCPBucketUpload

    for variable in upload_variables:
        upload_vars = GCPBucketUpload(
            task_id=f"upload_{variable}",
            bucket_name="gcp-modeling-dataset",
            bucket_cred="gcp-object",
            template_path=f"/data/forecast/ecmwf_as/{variable}/%Y/%j/ecmwf_as_{variable}_M000_%Y%m%d00.nc",
        )
