from airflow.decorators import task_group

@task_group
def hydra(date_string, run_hour):
    from operators.Hydra import HydraOperator
    from helpers.check_files.checks import callback_calc_hydra


    date_hydra = f"{{{{ {date_string}.strftime('%Y%m%d{run_hour}') }}}}"

    msg2nc = HydraOperator(
        task_id="msg2nc",
        process="msg2nc",
        date=date_hydra,
        process_other={"--no-tmp": ""},
        versao="develop",
    )
    mergetime = HydraOperator(
        task_id="mergetime",
        process="mergetime",
        date=date_hydra,
        process_other={"--no-tmp": ""},
        versao="develop",
    )
    calc = HydraOperator(
        task_id="calc",
        process="calc",
        var=[
            "100m_wind_direction",
            "100m_wind_speed",
            "10m_wind_direction",
            "10m_wind_speed",
            "2m_relative_humidity",
        ],
        date=date_hydra,
        process_other={"--no-tmp": ""},
        on_execute_callback=callback_calc_hydra,
        versao="develop",  
    )
    msg2nc >> mergetime >> calc