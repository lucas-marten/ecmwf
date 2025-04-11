from airflow.decorators import task_group


@task_group(group_id="hydra_priority", tooltip="hydra_priority")
def hydra(date_string, run_hour):
    from helpers.check_files.checks import callback_calc_hydra
    from operators.Hydra import HydraOperator

    date_hydra = f"{{{{ {date_string}.strftime('%Y%m%d{run_hour}') }}}}"

    msg2nc = HydraOperator(
        task_id="msg2nc",
        process="msg2nc",
        date=date_hydra,
        process_other={"-np": "16"},
    )
    mergetime = HydraOperator(
        task_id="mergetime",
        process="mergetime",
        date=date_hydra,
        process_other={"-np": "8"},
    )
    hydra_joules_radiation_convert = HydraOperator(
        task_id="hydra_joules_radiation_convert",
        process=["joules_radiation_convert", "calc"],
        date=date_hydra,
        process_other={"-np": "3"},
    )
    hydra_calc = HydraOperator(
        task_id="hydra_calc",
        process="calc",
        date=date_hydra,
        process_other={"-np": "3"},
        on_execute_callback=callback_calc_hydra,
    )
    msg2nc >> mergetime >> [hydra_joules_radiation_convert, hydra_calc]