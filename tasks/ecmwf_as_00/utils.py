import bz2
import os
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool

import botocore
import gspread
import numpy as np
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials


def parse_date(date_string, run_hour, context):
    date = datetime.fromisoformat(str(context[date_string]))
    return date.replace(tzinfo=None, hour=int(run_hour))


def check_s3_file(prefix, s3):
    try:
        s3.Object("ecmwf-upload", prefix).load()
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            raise FileExistsError(f"Arquivo não encontrado na AWS: {prefix}")
    print(f"Arquivo AWS OK: {prefix}")
    return True


def generate_file_path(date, prefix, time, output_dir):
    if prefix in ["S7S", "S7D", "CLE"]:
        file_name = time.strftime(f"{prefix}%m%d%H00%m%d%H001")
    elif prefix == "D3F":
        file_name = time.strftime(f"{prefix}%m%d0000%m%d____1")
    else:
        raise KeyError("Prefixo inválido")
    return os.path.join(output_dir, file_name)


def get_sheet(sheet_name, tab):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "/airflow/base/credentials/gcp-object.json", scope
    )
    client = gspread.authorize(creds)
    return client.open(sheet_name).worksheet(tab)

def sheet_client(files, sheet):
    sheet = get_sheet("airflow_clients", sheet)
    for file in files:
        try:
            collun = sheet.findall(f"file:{file}")[-1].col
            cont = len(sheet.col_values(collun))
            sheet.update_cell(
                cont + 1, collun, datetime.now().strftime("%H:%M %d/%m/%Y UTC")
            )
        except Exception as e:
            print(f"Error : {str(e)}")
    return True

def clear_raws(output_dir, prefix):

    for f in os.listdir(output_dir):
        if f.startswith(prefix):
            path_rm = os.path.join(output_dir, f)
            if os.path.isfile(path_rm):
                print("removing:", path_rm)
                os.remove(path_rm)
    return True


def get_ec_times(date, prefix):

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
        print(f"{prefix} @ get_ec_times() times lenght: {len(times)}")

    elif prefix == "S7S":
        times = pd.date_range(
            date + timedelta(days=0, hours=1),
            date + timedelta(days=3, hours=0),
            freq="1H",
        )
        print(f"{prefix} @ get_ec_times() times lenght: {len(times)}")

    elif prefix == "CLE":
        t1 = pd.date_range(date, date + timedelta(days=5, hours=21), freq="3H")
        t2 = pd.date_range(
            date + timedelta(days=6), date + timedelta(days=9), freq="6H"
        )
        times = pd.to_datetime(np.concatenate([t1, t2]))
        print(f"{prefix} @ get_ec_times() times lenght: {len(times)}")

    elif prefix == "D3F":
        times = pd.date_range(
            date + timedelta(days=15), date + timedelta(days=31), freq="1D"
        )
        print(f"{prefix} @ get_ec_times() times lenght: {len(times)}")

    else:
        raise KeyError("get_ec_times(): Prefixo inválido")

    return times


def get_payload(date, prefix, s3, times):
    if prefix in ["S7S", "S7D", "CLE"]:
        prefix = date.strftime(f"{prefix}%m%d%H")
        payload = list(
            map(lambda x: (x.strftime(f"{prefix}00%m%d%H001.bz2"), s3), times)
        )
        return payload

    elif prefix == "D3F":
        prefix = date.strftime(f"{prefix}%m%d")
        payload = list(
            map(lambda x: (x.strftime(f"{prefix}0000%m%d____1.bz2"), s3), times)
        )
        return payload

    else:
        raise KeyError("check_s3(): Prefixo inválido")


def decompress(output_dir, prefix):

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


# if __name__ == "__main__":
#    import argparse
#    from datetime import datetime
#
#    functions = {
#        "get_ec_times": get_ec_times,
#        "check_download": check_download,
#        "check_s3": check_s3,
#        "decompress": decompress,
#        "ct_datahub_decompression": ct_datahub_decompression,
#    }
#
#    description = """This script create a netCDF4"""
#
#    parser = argparse.ArgumentParser(description=description)
#    parser.add_argument(
#        "-d",
#        "--date",
#        help="Date in %Y%m%dT%H%M format",
#        action="store",
#        required=False,
#        dest="date",
#        default=datetime.now().strftime("%Y%m%dT%H%M"),
#    )
#    parser.add_argument(
#        "-hr",
#        "--hour",
#        help="Hour in %H format",
#        action="store",
#        required=False,
#        dest="hour",
#        default="00",
#    )
#    parser.add_argument(
#        "-pf",
#        "--prefix",
#        help="Prefix for ECMWF",
#        action="store",
#        required=False,
#        dest="prefix",
#        choices=["S7D", "S7S", "CLE", "D3F"],
#    )
#    parser.add_argument(
#        "-tf",
#        "--test_function",
#        help="Function to test",
#        action="store",
#        required=False,
#        dest="test_function",
#    )
#    parser.add_argument(
#        "-of",
#        "--output_dir",
#        help="Output files directory",
#        action="store",
#        required=True,
#        dest="output_dir",
#    )
#
#    args = vars(parser.parse_args())
#
#    test_function = functions.get(args["test_function"])
#    test_function(**args)
#
#    exit(0)
