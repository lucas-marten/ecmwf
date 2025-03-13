from datetime import timedelta

from airflow.decorators import task, task_group
from airflow.operators.bash import BashOperator
from helpers.check_files.checks import callback_calc_hydra
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
        "cape_index",
        "sfc_visibility",
    ],
}


@task_group(group_id="hydra_priority_group", tooltip="hydra_priority_group")
def hydra_priority_group(date_str):
    msg2nc = HydraOperator(
        task_id="msg2nc",
        process="msg2nc",
        date=date_str,
        process_other={"-np": "16"},
        var=variables,
    )

    mergetime = HydraOperator(
        task_id="mergetime",
        process="mergetime",
        date=date_str,
        process_other={"-np": "8"},
        var=variables,
    )

    msg2nc >> mergetime


@task_group(group_id="k_tt_sweat_indexes", tooltip="k_tt_sweat_indexes")
def k_tt_sweat_indexes(date_str):
    for variable in ["k_index", "total_totals_index", "sweat_index"]:
        _task = HydraOperator(
            task_id=f"hydra_grib_{variable}",
            process=["grib_calc", "mergetime"],
            var=variable,
            date=date_str,
            process_other={"-np": "3"},
        )
        _task


@task_group(group_id="hydra_group", tooltip="hydra_group")
def hydra_group(date_str):
    calc_10m_wind_speed = HydraOperator(
        task_id="calc_10m_wind_speed",
        process="calc",
        date=date_str,
        process_other={"-np": "3"},
        var=["10m_wind_speed"],
        on_execute_callback=callback_calc_hydra,
    )

    hydra_joules_radiation_convert = HydraOperator(
        task_id="hydra_joules_radiation_convert",
        process=["joules_radiation_convert", "calc"],
        date=date_str,
        process_other={"-np": "3"},
    )

    @task_group(group_id="hydra_secondarys_group", tooltip="hydra_secondarys_group")
    def hydra_secondarys_group():
        for name, variables in second_variables.items():
            hydra_msg2nc = HydraOperator(
                task_id=f"msg2nc_{name}",
                process="msg2nc",
                date=date_str,
                process_other={"-np": "8"},
                var=variables,
            )
            hydra_mergetime = HydraOperator(
                task_id=f"mergetime_{name}",
                process="mergetime",
                date=date_str,
                process_other={"-np": "8"},
                var=variables,
            )

            hydra_msg2nc >> hydra_mergetime

    @task_group(group_id="hydra_calc_group", tooltip="hydra_calc_group")
    def hydra_calc_group():

        hydra_calc = HydraOperator(
            task_id="hydra_calc",
            process="calc",
            date=date_str,
            process_other={"-np": "3"},
            on_execute_callback=callback_calc_hydra,
        )

        hydra_850hPa_wind_speed = HydraOperator(
            task_id="hydra_850hPa_wind_speed",
            process="calc",
            date=date_str,
            var=["850hPa_wind_speed"],
            process_other={"-np": "3"},
            on_execute_callback=callback_calc_hydra,
        )

        @task_group(group_id="fog_and_frost_index", tooltip="fog_and_frost_index")
        def fog_and_frost_index():
            for variable in ["fog_stability_index", "sfc_frost_index"]:
                _task = HydraOperator(
                    task_id=variable,
                    process="calc",
                    date=date_str,
                    var=variable,
                    process_other={"-np": "3"},
                    on_execute_callback=callback_calc_hydra,
                )
                _task

        leaf_wetting_convert = HydraOperator(
            task_id="hydra_leaf_wetting_convert",
            process="leaf_wetting_convert",
            date=date_str,
            process_other={"-np": "3"},
        )

        lightning = HydraOperator(
            task_id="lightning",
            process="calc",
            var=["lightning"],
            date=date_str,
            process_other={"-np": "3"},
            on_execute_callback=callback_calc_hydra,
        )

        rain_rate = HydraOperator(
            task_id="rain_rate",
            process="rain_rate",
            date=date_str,
            execution_timeout=timedelta(minutes=120),
        )

        risk_mosquito = BashOperator(
            task_id="mosquito_risk",
            bash_command=f"cd /airflow/tools/hydra/tools/ && ./mosquito_risk.py --model ecmwf_as --reference ct_observed_as -d {date_str} -k forecast",
        )

        calc_alpha = BashOperator(
            task_id="calc_alpha",
            bash_command=f"cd /airflow/tools/hydra/tools/ && ./calc_alpha.py --target ecmwf_as --no-tmp --kind forecast -d {date_str}",
        )

        (
            hydra_calc
            >> hydra_850hPa_wind_speed
            >> fog_and_frost_index()
            >> leaf_wetting_convert
            >> lightning
            >> risk_mosquito
            >> rain_rate
            >> calc_alpha
        )

    @task_group(
        group_id="hydra_wind_gust_height_group", tooltip="hydra_wind_gust_height_group"
    )
    def hydra_wind_gust_height_group():
        for height in [100]:
            _task = HydraOperator(
                task_id=f"hydra_{height}m",
                process="calc",
                date=date_str,
                var=[f"{height}m_wind_gust"],
                process_other={"--no-tmp": ""},
                execution_timeout=timedelta(minutes=120),
            )
            _task

    (
        [
            hydra_joules_radiation_convert,
            calc_10m_wind_speed,
            hydra_secondarys_group(),
        ]
        >> hydra_calc_group()
        >> hydra_wind_gust_height_group()
    )
