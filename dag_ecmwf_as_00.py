from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

from ecmwf_dev.helpers.utils import (check_download, check_s3,
                                        ct_datahub_decompression)
from ecmwf_dev.statics.dag_config import configs
from helpers.check_files.checks import callback_calc_hydra
from operators.Hydra import HydraOperator

with DAG(**configs) as dag:

    date = '{{{{(data_interval_end).strftime("{cmd}")}}}}'
    date_str = date.format(cmd="%Y%m%d00")

    with TaskGroup(f"collect_group", tooltip=f"collect_group") as collect_group:

        for prefix in ["S7D", "S7S"]:

            kwargs = {
                "prefix": prefix,
                "hour": "00",
                "output_dir": "/data/forecast/ecmwf_as/RAW/%Y/%j/00/",
            }

            task_check_s3 = PythonOperator(
                task_id=f"check_s3_{prefix}",
                python_callable=check_s3,
                op_kwargs=kwargs,
            )
            task_download_decompression = PythonOperator(
                task_id=f"download_{prefix}",
                python_callable=ct_datahub_decompression,
                op_kwargs=kwargs,
            )
            task_check_download = PythonOperator(
                task_id=f"check_download_{prefix}",
                python_callable=check_download,
                op_kwargs=kwargs,
            )
            task_check_s3 >> task_download_decompression >> task_check_download


    with TaskGroup(f"sema_group", tooltip=f"sema_group") as sema_group:
        cmd_extract = date.format(
            cmd="/airflow/dags/helpers/forecast/send_sema_extract.py  --category forecast --model ecmwf_as --date %Y-%m-%dT00:00:00 --stackdir /airflow/dags/helpers/forecast/statics/sema_extract/ --type csvs"
        )
        cmd_zip = date.format(
            cmd="/airflow/dags/helpers/forecast/send_sema_extract.py  --category forecast --model ecmwf_as --date %Y-%m-%dT00:00:00 --stackdir /airflow/dags/helpers/forecast/statics/sema_extract/ --type zip"
        )
        sema_extraction = BashOperator(
            task_id="sema_extraction",
            bash_command=cmd_extract,
        )
        sema_zip = BashOperator(
            task_id="sema_zip",
            bash_command=cmd_zip,
        )
        sema_mail = HydraOperator(
            task_id=f"sema_mail",
            target="sema_extractions",
            process="send_mail",
            client="ecmwf10",
            date=date_str,
        )
        sema_extraction >> sema_zip >> sema_mail

    with TaskGroup("hydra_priority_group", tooltip="hydra_priority_group") as hydra_priority_group:
        variables = [
            "total_precipitation",
            "2m_air_temperature",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "10m_wind_gust",
            "2m_dew_point_temperature",
            "downward_short_wave_radiation",
            "msl_pressure",
            "sfc_pressure",
            "total_cloud_cover",
        ]

        task_msg2nc = HydraOperator(
            task_id="hydra_msg2nc",
            process="msg2nc",
            date=date_str,
            process_other={"-np": "16"},
            var=variables,
        )
        task_mergetime = HydraOperator(
            task_id="hydra_mergetime",
            process="mergetime",
            date=date_str,
            process_other={"-np": "8"},
            var=variables,
        )
        task_msg2nc >> task_mergetime

    with TaskGroup("hydra", tooltip="hydra") as hydra:

        task_10m_wind_speed = HydraOperator(
            task_id="hydra_calc_10m_wind_speed",
            process="calc",
            date=date_str,
            process_other={"-np": "3"},
            var=["10m_wind_speed"],
            on_execute_callback=callback_calc_hydra,
        )
        task_joules_radiation_convert = HydraOperator(
            task_id="hydra_joules_radiation_convert",
            process=["joules_radiation_convert", "calc"],
            date=date_str,
            process_other={"-np": "3"},
        )

        with TaskGroup(
            "hydra_secondarys_group", tooltip="hydra_secondarys_group"
        ) as hydra_secondarys_group:

            var_second = [
                "precipitation_type",
                "{level}hPa_air_temperature",
                "net_sfc_solar_radiation",
                "100m_u_component_of_wind",
                "100m_v_component_of_wind",
                "high_cloud_cover",
                "mid_cloud_cover",
                "low_cloud_cover",
                "cape_index",
                "sfc_visibility",
                "{level}hPa_u_component_of_wind",
                "{level}hPa_v_component_of_wind",
                "{level}hPa_relative_humidity",
                "{level}hPa_geopotential_height",
            ]

            Hydra_msg2nc = HydraOperator(
                task_id="Hydra_msg2nc",
                process="msg2nc",
                date=date_str,
                process_other={"-np": "8"},
                var=var_second,
            )
            Hydra_mergetime = HydraOperator(
                task_id="Hydra_mergetime",
                process="mergetime",
                date=date_str,
                process_other={"-np": "8"},
                var=var_second,
            )
            Hydra_msg2nc >> Hydra_mergetime

        with TaskGroup(
            "hydra_calc_group", tooltip="hydra_calc_group"
        ) as hydra_calc_group:
            task_calc_primary = HydraOperator(
                task_id="hydra_calc_primary",
                process="calc",
                date=date_str,
                process_other={"-np": "3"},
                on_execute_callback=callback_calc_hydra,
            )
            task_calc_force = HydraOperator(
                task_id="hydra_calc_force",
                process="calc",
                date=date_str,
                var=["850hPa_wind_speed"],
                process_other={"-np": "3"},
                on_execute_callback=callback_calc_hydra,
            )
            task_calc_force1 = HydraOperator(
                task_id="hydra_calc_force1",
                process="calc",
                date=date_str,
                var=["fog_stability_index", "sfc_frost_index"],
                process_other={"-np": "3"},
                on_execute_callback=callback_calc_hydra,
            )
            task_leaf_wetting_convert = HydraOperator(
                task_id="hydra_leaf_wetting_convert",
                process="leaf_wetting_convert",
                date=date_str,
                process_other={"-np": "3"},
            )
            task_grib_calc = HydraOperator(
                task_id="hydra_grib_calc",
                process=["grib_calc", "mergetime"],
                var=["k_index", "total_totals_index", "sweat_index"],
                date=date_str,
                process_other={"-np": "3"},
            )
            task_pos_calc = HydraOperator(
                task_id="hydra_pos_calc",
                process="calc",
                var=["lightning"],
                date=date_str,
                process_other={"-np": "3"},
                on_execute_callback=callback_calc_hydra,
            )
            task_rain_rate = HydraOperator(
                task_id="hydra_rain_rate",
                process="rain_rate",
                date=date_str,
                execution_timeout=timedelta(minutes=120),
            )
            task_risk_mosquito = BashOperator(
                task_id="hydra_mosquito_risk",
                bash_command=date.format(
                    cmd="cd /airflow/tools/hydra/tools/ && ./mosquito_risk.py --model ecmwf_as --reference ct_observed_as -d %Y%m%d00 -k forecast"
                ),
            )
            task_calc_alpha = BashOperator(
                task_id="hydra_calc_alpha",
                bash_command=date.format(
                    cmd="cd /airflow/tools/hydra/tools/ && ./calc_alpha.py --target ecmwf_as --no-tmp --kind forecast -d %Y%m%d00"
                ),
            )

            (
                task_calc_primary
                >> task_calc_force
                >> task_calc_force1
                >> task_leaf_wetting_convert
                >> task_grib_calc
                >> task_pos_calc
                >> task_risk_mosquito
                >> task_rain_rate
                >> task_calc_alpha
            )

        with TaskGroup(
            f"hydra_wind_height_group", tooltip=f"hydra_wind_height_group"
        ) as hydra_wind_height_group:
            for height in [100]:
                task_hydra_height = HydraOperator(
                    task_id=f"hydra_height_{height}m",
                    process="calc",
                    date=date_str,
                    var=[
                        f"{height}m_wind_gust",
                    ],
                    process_other={"--no-tmp": ""},
                    execution_timeout=timedelta(minutes=120),
                )

        (
            collect_group
            >> hydra_priority_group
            >> hydra_secondarys_group
            >> hydra_calc_group
            >> hydra_wind_height_group
        )

    collect_group >> hydra_priority_group >> sema_group
    collect_group >> hydra_priority_group >> hydra
