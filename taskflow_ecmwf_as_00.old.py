from airflow.decorators import dag, task, task_group
from ecmwf_dev.helpers.utils import (
    check_download, check_s3, ct_datahub_decompression
)
from ecmwf_dev.statics.taskflow_config import configs
from helpers.check_files.checks import callback_calc_hydra
from operators.Hydra import HydraOperator
from airflow.operators.bash import BashOperator

@dag(**configs)
def ecmwf_as_00_dev_taskflow():
    date_str = "{{ data_interval_end.strftime('%Y%m%d00') }}"

    @task_group
    def collect_group():
        @task
        def task_check_s3(prefix):
            return check_s3(prefix=prefix, hour="00", output_dir="/data/forecast/ecmwf_as/RAW/%Y/%j/00/")

        @task
        def task_download_decompression(prefix):
            return ct_datahub_decompression(prefix=prefix, hour="00", output_dir="/data/forecast/ecmwf_as/RAW/%Y/%j/00/")

        @task
        def task_check_download(prefix):
            return check_download(prefix=prefix, hour="00", output_dir="/data/forecast/ecmwf_as/RAW/%Y/%j/00/")

        for prefix in ["S7D", "S7S"]:
            task_check_s3(prefix) >> task_download_decompression(prefix) >> task_check_download(prefix)

    @task_group
    def sema_group():
        cmd_extract = f"/airflow/dags/helpers/forecast/send_sema_extract.py  --category forecast --model ecmwf_as --date {date_str} --stackdir /airflow/dags/helpers/forecast/statics/sema_extract/ --type csvs"
        cmd_zip = f"/airflow/dags/helpers/forecast/send_sema_extract.py  --category forecast --model ecmwf_as --date {date_str} --stackdir /airflow/dags/helpers/forecast/statics/sema_extract/ --type zip"

        sema_extraction = BashOperator(task_id="sema_extraction", bash_command=cmd_extract)
        sema_zip = BashOperator(task_id="sema_zip", bash_command=cmd_zip)
        sema_mail = HydraOperator(task_id="sema_mail", target="sema_extractions", process="send_mail", client="ecmwf10", date=date_str)
        
        sema_extraction >> sema_zip >> sema_mail

    @task_group
    def hydra_priority_group():
        variables = [
            "total_precipitation", "2m_air_temperature", "10m_u_component_of_wind", "10m_v_component_of_wind",
            "10m_wind_gust", "2m_dew_point_temperature", "downward_short_wave_radiation", "msl_pressure",
            "sfc_pressure", "total_cloud_cover"
        ]

        task_msg2nc = HydraOperator(task_id="hydra_msg2nc", process="msg2nc", date=date_str, process_other={"-np": "16"}, var=variables)
        task_mergetime = HydraOperator(task_id="hydra_mergetime", process="mergetime", date=date_str, process_other={"-np": "8"}, var=variables)
        task_msg2nc >> task_mergetime


    @task_group
    def hydra_task_group():
        date_str = "{{ data_interval_end.strftime('%Y%m%d00') }}"
        
        @task(task_id="hydra_calc_10m_wind_speed")
        def hydra_calc_10m_wind_speed():
            return HydraOperator(
                task_id="hydra_calc_10m_wind_speed",
                process="calc",
                date=date_str,
                process_other={"-np": "3"},
                var=["10m_wind_speed"],
                on_execute_callback=callback_calc_hydra,
            )
        
        @task(task_id="hydra_joules_radiation_convert")
        def hydra_joules_radiation_convert():
            return HydraOperator(
                task_id="hydra_joules_radiation_convert",
                process=["joules_radiation_convert", "calc"],
                date=date_str,
                process_other={"-np": "3"},
            )
        
        @task_group(group_id="hydra_secondarys_group", tooltip="hydra_secondarys_group")
        def hydra_secondarys_group():
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
        
        @task_group()
        def hydra_calc_group():
            
            @task()
            def hydra_calc():
                return HydraOperator(
                    task_id="hydra_calc",
                    process="calc",
                    date=date_str,
                    process_other={"-np": "3"},
                    on_execute_callback=callback_calc_hydra,
                )
            
            @task()
            def hydra_850hPa_wind_speed():
                return HydraOperator(
                    task_id="hydra_850hPa_wind_speed",
                    process="calc",
                    date=date_str,
                    var=["850hPa_wind_speed"],
                    process_other={"-np": "3"},
                    on_execute_callback=callback_calc_hydra,
                )
            
            @task_group()
            def fog_and_frost_index():
                for variable in ["fog_stability_index", "sfc_frost_index"]:
                    task = HydraOperator(
                        task_id=f"hydra_{variable}",
                        process="calc",
                        date=date_str,
                        var=variable,
                        process_other={"-np": "3"},
                        on_execute_callback=callback_calc_hydra,
                    )
            
            @task()
            def leaf_wetting_convert():
                return HydraOperator(
                    task_id="hydra_leaf_wetting_convert",
                    process="leaf_wetting_convert",
                    date=date_str,
                    process_other={"-np": "3"},
                )
            
            @task_group()
            def k_tt_sweat_indexes():
                for variable in ["k_index", "total_totals_index", "sweat_index"]:
                    task = HydraOperator(
                        task_id=f"hydra_grib_{variable}",
                        process=["grib_calc", "mergetime"],
                        var=variable,
                        date=date_str,
                        process_other={"-np": "3"},
                    )
            
            @task()
            def lightning():
                return HydraOperator(
                    task_id="lightning",
                    process="calc",
                    var=["lightning"],
                    date=date_str,
                    process_other={"-np": "3"},
                    on_execute_callback=callback_calc_hydra,
                )
            
            @task()
            def rain_rate():
                return HydraOperator(
                    task_id="rain_rate",
                    process="rain_rate",
                    date=date_str,
                    execution_timeout=timedelta(minutes=120),
                )
                   
            @task()
            def risk_mosquito():
                return BashOperator(
                task_id="mosquito_risk",
                bash_command=f"cd /airflow/tools/hydra/tools/ && ./mosquito_risk.py --model ecmwf_as --reference ct_observed_as -d {date_str} -k forecast"
            )
            
            @task()
            def calc_alpha():
                return BashOperator(
                task_id="calc_alpha",
                bash_command=f"cd /airflow/tools/hydra/tools/ && ./calc_alpha.py --target ecmwf_as --no-tmp --kind forecast -d {date_str}"
            )
            
            (hydra_calc() >> hydra_850hPa_wind_speed() >> fog_and_frost_index() >>
            k_tt_sweat_indexes() >> leaf_wetting_convert() >> lightning() >>
            risk_mosquito() >> rain_rate() >> calc_alpha())

        @task_group(group_id="hydra_wind_height_group", tooltip="hydra_wind_height_group")
        def hydra_wind_height_group(date_str):
            for height in [100]:
                HydraOperator(
                    task_id=f"hydra_height_{height}m",
                    process="calc",
                    date=date_str,
                    var=[f"{height}m_wind_gust"],
                    process_other={"--no-tmp": ""},
                    execution_timeout=timedelta(minutes=120),
                )
        
        for _task in [hydra_joules_radiation_convert() >> hydra_calc_10m_wind_speed()]:
            _task >> hydra_secondarys_group() >> hydra_calc_group()

    collect_group() >> hydra_priority_group() >> [hydra_task_group(), sema_group()]

ecmwf_as_00_dev_taskflow()
