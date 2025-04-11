from airflow.decorators import task, task_group


@task_group(group_id="hydra_priority_group", tooltip="hydra_priority_group")
def hydra_priority_group(date_string, run_hour):
    from operators.Hydra import HydraOperator


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

    date_hydra = f"{{{{ {date_string}.strftime('%Y%m%d{run_hour}') }}}}"

    msg2nc = HydraOperator(
        task_id="msg2nc",
        process="msg2nc",
        date=date_hydra,
        process_other={"-np": "16"},
        var=variables,
    )

    mergetime = HydraOperator(
        task_id="mergetime",
        process="mergetime",
        date=date_hydra,
        process_other={"-np": "8"},
        var=variables,
    )
    msg2nc >> mergetime


@task_group(group_id="hydra_group", tooltip="hydra_group")
def hydra_group(date_string, run_hour):
    from datetime import timedelta

    from airflow.operators.bash import BashOperator

    from helpers.check_files.checks import callback_calc_hydra
    from operators.Hydra import HydraOperator


    date_hydra = f"{{{{ {date_string}.strftime('%Y%m%d{run_hour}') }}}}"

    second_variables = {
        "level": [
            "{level}hPa_air_temperature",
            "{level}hPa_u_component_of_wind",
            "{level}hPa_v_component_of_wind",
            "{level}hPa_relative_humidity",
            "{level}hPa_geopotential_height",
        ],
        "height": ["100m_u_component_of_wind", "100m_v_component_of_wind"],
        "cloud": ["high_cloud_cover", "mid_cloud_cover", "low_cloud_cover"],
        "others": [
            "precipitation_type",
            "net_sfc_solar_radiation",
            # "cape_index",
            "sfc_visibility",
        ],
    }

    rain_rate = HydraOperator(
        task_id="rain_rate",
        process="rain_rate",
        date=date_hydra,
        execution_timeout=timedelta(minutes=120),
    )

    @task_group(group_id="hydra_secondarys_group", tooltip="hydra_secondarys_group")
    def hydra_secondarys_group():

        for name, variables in second_variables.items():
            hydra_msg2nc = HydraOperator(
                task_id=f"msg2nc_{name}",
                process="msg2nc",
                date=date_hydra,
                process_other={"-np": "8"},
                var=variables,
            )
            hydra_mergetime = HydraOperator(
                task_id=f"mergetime_{name}",
                process="mergetime",
                date=date_hydra,
                process_other={"-np": "8"},
                var=variables,
            )

            hydra_msg2nc >> hydra_mergetime

    @task_group(group_id="hydra_calc_group", tooltip="hydra_calc_group")
    def hydra_calc_group():

        index_gribs_task = HydraOperator(
            task_id=f"hydra_grib_indexes",
            process=["grib_calc", "mergetime"],
            var=["k_index", "total_totals_index", "sweat_index"],
            date=date_hydra,
            process_other={"-np": "3"},
        )
        leaf_wetting_convert = HydraOperator(
            task_id="hydra_leaf_wetting_convert",
            process="leaf_wetting_convert",
            date=date_hydra,
            process_other={"-np": "3"},
        )

        if run_hour == "00":
            hydra_calc = HydraOperator(
                task_id="hydra_calc",
                process="calc",
                date=date_hydra,
                process_other={"-np": "3"},
                on_execute_callback=callback_calc_hydra,
            )

            hydra_850hPa_wind_speed = HydraOperator(
                task_id="hydra_850hPa_wind_speed",
                process="calc",
                date=date_hydra,
                var=["850hPa_wind_speed"],
                process_other={"-np": "3"},
                on_execute_callback=callback_calc_hydra,
            )

            fog_and_frost_index_task = HydraOperator(
                task_id="fog_and_frost_index",
                process="calc",
                date=date_hydra,
                var=["fog_stability_index", "sfc_frost_index"],
                process_other={"-np": "3"},
                on_execute_callback=callback_calc_hydra,
            )

            risk_mosquito = BashOperator(
                task_id="mosquito_risk",
                bash_command=f"cd /airflow/tools/hydra/tools/ && ./mosquito_risk.py --model ecmwf_as --reference ct_observed_as -d {date_hydra} -k forecast",
            )
            index_gribs_task
            (
                hydra_calc
                >> hydra_850hPa_wind_speed
                >> fog_and_frost_index_task
                >> leaf_wetting_convert
                >> risk_mosquito
            )

        else:
            index_gribs_task
            leaf_wetting_convert

    @task_group(group_id="hydra_wind_group", tooltip="hydra_wind_group")
    def hydra_wind_group():
        hydra_100m_direction = HydraOperator(
            task_id=f"hydra_100m_direction",
            process="calc",
            date=date_hydra,
            var=[f"100m_wind_direction"],
            process_other={"--no-tmp": ""},
            execution_timeout=timedelta(minutes=120),
        )
        hydra_100m_speed = HydraOperator(
            task_id=f"hydra_100m_speed",
            process="calc",
            date=date_hydra,
            var=[f"100m_wind_speed"],
            process_other={"--no-tmp": ""},
            execution_timeout=timedelta(minutes=120),
        )
        hydra_100m_gust = HydraOperator(
            task_id=f"hydra_100m_gust",
            process="calc",
            date=date_hydra,
            var=[f"100m_wind_gust"],
            process_other={"--no-tmp": ""},
            execution_timeout=timedelta(minutes=120),
        )
        calc_alpha = BashOperator(
            task_id="calc_alpha",
            bash_command=f"cd /airflow/tools/hydra/tools/ && ./calc_alpha.py --target ecmwf_as --no-tmp --kind forecast -d {date_hydra}",
        )
        hydra_100m_direction >> hydra_100m_speed >> hydra_100m_gust >> calc_alpha

    calc_10m_wind_speed = HydraOperator(
        task_id="calc_10m_wind_speed",
        process="calc",
        date=date_hydra,
        process_other={"-np": "3"},
        var=["10m_wind_speed"],
        on_execute_callback=callback_calc_hydra,
    )

    hydra_joules_radiation_convert = HydraOperator(
        task_id="hydra_joules_radiation_convert",
        process=["joules_radiation_convert", "calc"],
        date=date_hydra,
        process_other={"-np": "3"},
    )

    rain_rate
    (
        [hydra_joules_radiation_convert, calc_10m_wind_speed]
        >> hydra_secondarys_group()
        >> hydra_calc_group()
        >> hydra_wind_group()
    )
