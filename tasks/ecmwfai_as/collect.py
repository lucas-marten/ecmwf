from airflow.decorators import dag, task, task_group


@task_group
def collect(date_string, run_hour):
    import os
    from datetime import datetime
    import logging as log
    import requests
    from urllib.error import HTTPError
    from multiprocessing.dummy import Pool

    @task()
    def download(date_string, run_hour, **context):

        def _download(file_in, file_out):
            try:
                with requests.get(file_in, stream=True, verify=False) as response:
                    response.raise_for_status()
                    with open(file_out, 'wb') as out_file:
                        for chunk in response.iter_content(chunk_size=8192):
                            out_file.write(chunk)
            except HTTPError as e:
                raise e
            else:
                log.info(f"download_ftp(): Arquivo baixado: {file_out}")
            return True

        date = datetime.fromisoformat(str(context[date_string]))
        date = date.replace(tzinfo=None)
        list_ftp = []
        raw_path = date.strftime(f"/data/forecast/ecmwfai_as/RAW/%Y/%j/{run_hour}/")
        os.makedirs(raw_path, exist_ok=True)

        for i in range(0, 361, 6):

            url_base = date.strftime(f"https://data.ecmwf.int/forecasts/%Y%m%d/{run_hour}z/aifs-single/0p25/oper/")
            file = date.strftime(f"%Y%m%d{run_hour}0000-{i}h-oper-fc.grib2")

            list_ftp.append([url_base+file, raw_path+file])

        log.warning(f"Check_ftp(): lista de: {len(list_ftp)}")
        with Pool(processes=8) as pool:
            pool.starmap(_download, list_ftp)
        return


    @task
    def check(date_string, run_hour, **context):

        def _check(file_in):
            try:
                response = requests.head(file_in, timeout=100, verify=False)
                response.raise_for_status()
            except HTTPError as e:
                raise e
            else:
                log.info(f"Check_ftp(): Arquivo existe no ftp: {file_in}")
            return

        date = datetime.fromisoformat(str(context[date_string]))
        date = date.replace(tzinfo=None)
        list_ftp = []
        raw_path = date.strftime(f"/data/forecast/ecmwfai_as/RAW/%Y/%j/{run_hour}/")
        os.makedirs(raw_path, exist_ok=True)

        for i in range(0, 361, 6):

            url_base = date.strftime(f"https://data.ecmwf.int/forecasts/%Y%m%d/{run_hour}z/aifs-single/0p25/oper/")
            file = date.strftime(f"%Y%m%d{run_hour}0000-{i}h-oper-fc.grib2")

            list_ftp.append([url_base+file])

        log.warning(f"Check_ftp(): lista de: {len(list_ftp)}")
        with Pool(processes=8) as pool:
            pool.starmap(_check, list_ftp)
        return
    
    check(date_string, run_hour) >> download(date_string, run_hour)