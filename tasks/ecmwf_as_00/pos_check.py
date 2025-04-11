from airflow.decorators import task


@task
def task_check_dataset(date_string, run_hour, path, model, **context):
    import logging as log
    import os
    from datetime import datetime

    import yaml

    from ecmwf_dev.tasks.ecmwf_as_00.utils import get_sheet

    date = datetime.fromisoformat(str(context[date_string]))
    date = date.replace(tzinfo=None, hour=int(run_hour))
    raw_path = date.strftime(f"{path}RAW/%Y/%j/{run_hour}")

    log.info(f"check_dataset(): Deletando arquvivos RAW de 0 dias atrás: {raw_path}")

    if "RAW" in path:
        try:
            log.info(f"INFO: remove path...:\n-{path}")
            log.info(f"rm -r {path}/*")
            os.system(f"rm -r {path}/*")
        except Exception as error:
            log.info(f"ERROR...: \n-{str(error)}")

    msg2nc = date.strftime(f"{path}RAW/msg2nc/%m%d/{run_hour}")
    wrf = date.strftime(f"{path}RAW/wrf/%Y/%j")

    log.info(f"rm -r {raw_path}")
    os.system(f"rm -r {raw_path}")
    log.info(f"rm -rf {msg2nc}")
    os.system(f"rm -rf {msg2nc}")
    log.info(f"rm -rf {wrf}")
    os.system(f"rm -rf {wrf}")
    log.info(f"rm -rf {path}RAW/cut/*")
    os.system(f"rm -rf {path}RAW/cut/*")

    if model in ["ecmwf_as", "ecmwfe_as"]:
        log.info(f"rm -rf {path}RAW_tmp/*")
        os.system(f"rm -rf {path}RAW_tmp/*")

    files_fail = []
    var_fail = False

    with open(f"/airflow/dags/yamls/{model}.yml", "r") as file:
        try:
            cfg = yaml.safe_load(file)
        except yaml.YAMLError as e:
            raise e

    for var in cfg["var_pos"]:

        for member in range(0, cfg["member"] + 1):

            file = date.strftime(
                f"{path}/{var}/%Y/%j/{model}_{var}_M{member:03}_%Y%m%d{run_hour}.nc"
            )

            if os.path.isfile(file):
                log.info(f"dataset_check(): Arquivo já está em: {file}")
            else:
                var_fail = True
                files_fail.append(file)

    if var_fail:
        log.info(f"dataset_check(): Arquivo ainda nao está no local: {files_fail}")

    sheet = get_sheet("airflow_dataset", cfg["dataset"].title())
    sheet.update_cell(cfg["sheet"], 5, datetime.now().strftime("%H:%M %d/%m/%Y UTC"))
    return True
