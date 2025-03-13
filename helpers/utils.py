def get_time(hour, **context):
    from datetime import datetime

    try:
        date = datetime.fromisoformat(str(context["data_interval_end"]))
        date = date.replace(tzinfo=None, hour=int(hour))
    except:
        date = datetime.strptime(str(context["date"]), "%Y%m%dT%H%M")
        date = date.replace(tzinfo=None, hour=int(hour))
    return date


def get_ec_times(prefix, hour, **context):
    from datetime import timedelta

    import numpy as np
    import pandas as pd

    date = get_time(hour, **context)

    if prefix == "S7D":
        t1 = pd.date_range(
            date + timedelta(days=3, hours=3),
            date + timedelta(days=5, hours=21),
            freq="3H",
        )
        t2 = pd.date_range(
            date + timedelta(days=6, hours=0),
            date + timedelta(days=15, hours=0),
            freq="6H",
        )
        times = pd.to_datetime(np.concatenate([t1, t2]))

    elif prefix == "S7S":
        times = pd.date_range(
            date.replace(hour=1),
            date + timedelta(days=3, hours=0),
            freq="1H",
        )

    elif prefix == "CLE":
        t1 = pd.date_range(date, date + timedelta(days=5, hours=21), freq="3H")
        t2 = pd.date_range(
            date + timedelta(days=6), date + timedelta(days=9), freq="6H"
        )
        times = pd.to_datetime(np.concatenate([t1, t2]))

    elif prefix == "D3F":
        times = pd.date_range(
            date + timedelta(days=15), date + timedelta(days=31), freq="1D"
        )

    else:
        raise KeyError("get_ec_times(): Prefixo inválido")

    return times


def check_download(prefix, hour, output_dir, **context):
    import os

    date = get_time(hour, **context)
    output_dir = date.strftime(output_dir)

    times = get_ec_times(prefix, hour, **context)
    for time in times:

        if prefix in ["S7S", "S7D", "CLE"]:
            _prefix = date.strftime(f"{prefix}%m%d")
            file_name = time.strftime(f"{_prefix}{hour}00%m%d%H001")
            path_in = os.path.join(output_dir, file_name)

        elif prefix == "D3F":
            _prefix = date.strftime(f"{prefix}%m%d")
            file_name = time.strftime(f"{_prefix}0000%m%d____1")
            path_in = os.path.join(output_dir, file_name)

        else:
            raise KeyError("check_s3(): Prefixo inválido")

        if os.path.isfile(path_in):
            print(f"check_data(): Arquivo ainda está em: {path_in}")
        else:
            raise FileNotFoundError(
                f"check_data(): Arquivo ainda não está em: {path_in}"
            )
    return


def check_s3(prefix, hour, **context):
    from multiprocessing.dummy import Pool

    import botocore
    from boto3.session import Session

    def _check(prefix, s3):
        try:
            s3.Object("ecmwf-upload", prefix).load()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FileExistsError(
                    f"check_aws(): Sem arquivo na AWS, erro 404: {prefix}"
                )
        else:
            print(f"AWS_profile(): file AWS OK: {prefix}")
        return True

    s3 = Session(profile_name="stormgeo").resource("s3")

    date = get_time(hour, **context)

    times = get_ec_times(prefix, hour, **context)

    if prefix in ["S7S", "S7D", "CLE"]:
        prefix = date.strftime(f"{prefix}%m%d")
        payload = list(
            map(lambda x: (x.strftime(f"{prefix}{hour}00%m%d%H001.bz2"), s3), times)
        )

    elif prefix == "D3F":
        prefix = date.strftime(f"{prefix}%m%d")
        payload = list(
            map(lambda x: (x.strftime(f"{prefix}0000%m%d____1.bz2"), s3), times)
        )

    else:
        raise KeyError("check_s3(): Prefixo inválido")

    with Pool(processes=2) as pool:
        pool.starmap(_check, payload)

    return


def decompress(output_dir, prefix, **context):
    import bz2
    import os
    from multiprocessing.dummy import Pool

    def _decompress(file):
        with open(file, "rb") as source, open(file.replace(".bz2", ""), "wb") as dest:
            dest.write(bz2.decompress(source.read()))
            print(f"decompress done: {file}")
            if os.path.isfile(file):
                os.remove(file)
                print(f"removing: {file}")

    files = [
        os.path.join(output_dir, f)
        for f in os.listdir(output_dir)
        if f.startswith(prefix)
    ]

    if not files:
        raise FileNotFoundError(
            f"decompress(): Nenhum arquivo encontrado: {output_dir}"
        )

    with Pool(processes=10) as pool:
        pool.map(_decompress, files)

    return True


def ct_datahub_decompression(prefix, hour, output_dir, **context):
    import os

    from airflow.exceptions import AirflowException
    from airflow.operators.bash import BashOperator

    date = get_time(hour, **context)

    script = "python3 /airflow-dev/tools/ct-datahub/datahub/aws/download.py"
    output_dir = date.strftime(output_dir)

    for f in os.listdir(output_dir):
        if f.startswith(prefix):
            path_rm = os.path.join(output_dir, f)
            if os.path.isfile(path_rm):
                print("removing:", path_rm)
                os.remove(path_rm)

    cmd = date.strftime(
        f"{script} -pf {prefix}%m%d0000 -bn ecmwf-upload -of {output_dir} --download_all -pn stormgeo --date %Y%m%dT0000"
    )
    task = BashOperator(task_id=prefix, bash_command=cmd)
    task.execute(context=context)

    files = os.listdir(output_dir)

    if prefix == "S7D" and len(files) >= 60:
        decompress(output_dir, prefix)
    elif prefix == "S7S" and len(files) >= 73:
        decompress(output_dir, prefix)

    # TODO: testar para os outros ECMWFs
    # elif prefix == "CLE" and len(files) >= 73:
    #    decompress(output_dir, prefix)
    # elif prefix == "D3F" and len(files) >= 73:
    #    decompress(output_dir, prefix)

    else:
        raise AirflowException("ct_datahub(): Arquivos incompletos")

    return


if __name__ == "__main__":
    import argparse
    from datetime import datetime

    functions = {
        "get_ec_times": get_ec_times,
        "check_download": check_download,
        "check_s3": check_s3, 
        "decompress": decompress,
        "ct_datahub_decompression": ct_datahub_decompression,
    }

    description = """This script create a netCDF4"""

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-d",
        "--date",
        help="Date in %Y%m%dT%H%M format",
        action="store",
        required=False,
        dest="date",
        default=datetime.now().strftime("%Y%m%dT%H%M"),
    )
    parser.add_argument(
        "-hr",
        "--hour",
        help="Hour in %H format",
        action="store",
        required=False,
        dest="hour",
        default="00",
    )
    parser.add_argument(
        "-pf",
        "--prefix",
        help="Prefix for ECMWF",
        action="store",
        required=False,
        dest="prefix",
        choices=["S7D", "S7S", "CLE", "D3F"],
    )
    parser.add_argument(
        "-tf",
        "--test_function",
        help="Function to test",
        action="store",
        required=False,
        dest="test_function",
    )
    parser.add_argument(
        "-of",
        "--output_dir",
        help="Output files directory",
        action="store",
        required=True,
        dest="output_dir",
    )

    args = vars(parser.parse_args())

    test_function = functions.get(args["test_function"])
    test_function(**args)

    exit(0)
