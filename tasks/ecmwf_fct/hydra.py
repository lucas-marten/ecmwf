from airflow.decorators import task, task_group


@task_group()
def hydra(date_string):
    from operators.Hydra import HydraOperator
    from helpers.check_files.checks import callback_calc_hydra

    date_hydra = f"{{{{ {date_string}.strftime('%Y%m%d00') }}}}"

    msg2nc = HydraOperator(
        task_id="msg2nc",
        process="msg2nc",
        date=date_hydra,
    )
    mergetime = HydraOperator(
        task_id="mergetime",
        process="mergetime",
        date=date_hydra,
    )
    calc = HydraOperator(
        task_id="calc",
        process="calc",
        date=date_hydra,
        on_execute_callback=callback_calc_hydra,
    )
    msg2nc >> mergetime >> calc


    


