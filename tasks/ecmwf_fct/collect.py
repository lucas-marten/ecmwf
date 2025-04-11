from airflow.decorators import task, task_group


@task_group()
def collect(date_string):
    from datetime import datetime
    from boto3.session import Session
    import os
    import logging as log
    import botocore
    from ecmwf_dev.tasks.ecmwf_as_00.utils import decompress


    @task(task_id='download')
    def download(date_string, **context):
        
        date = datetime.fromisoformat(str(context[date_string])).replace(hour=0, minute=0, second=0)
        s3 = Session(profile_name='stormgeo').resource("s3")
        output_dir = date.strftime("/data/forecast/ecmwf_fct/RAW/%Y/%j/00/")
        
        os.makedirs(output_dir, exist_ok=True)
        prefix = date.strftime(f"S3F%m%d00")
        log.info(f"prefix: {prefix}")

        for file in s3.Bucket("ecmwf-upload").objects.filter(Prefix=prefix):
            file_out = os.path.join(output_dir, file.key)
            try:
                s3.Bucket("ecmwf-upload").download_file(file.key, file_out)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    raise FileExistsError(f"Down(): Sem arquivo na AWS, erro 404: {file.key}")
                else:
                    log.warning(f"Error connecting with s3, error:{str(e)}")
            else:
                log.info(f"Down(): Arquivo baixado: {file_out}")
            
        decompress(output_dir, prefix)
        return True
    
    @task(task_id='check')
    def check(**context):
        date = datetime.fromisoformat(str(context[date_string])).replace(hour=0, minute=0, second=0)
        output_dir = date.strftime("/data/forecast/ecmwf_fct/RAW/%Y/%j/00/")
        files = os.listdir(output_dir)

        if len(files) == 6:
            return True
        else:
            raise FileNotFoundError(f"download incomplete: {output_dir} - {files}")
    
    download(date_string) >> check()