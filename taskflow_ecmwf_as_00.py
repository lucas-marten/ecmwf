from airflow.decorators import dag
from ecmwf_dev.statics.taskflow_config import configs
from ecmwf_dev.helpers.hydra import hydra_priority_group
from ecmwf_dev.helpers.hydra import hydra_group
from ecmwf_dev.helpers.hydra import k_tt_sweat_indexes
from ecmwf_dev.helpers.collect import collect_group
from ecmwf_dev.helpers.sema import sema_group


@dag(**configs)
def ecmwf_as_00_dev_taskflow():
    date_str = "{{ data_interval_end.strftime('%Y%m%d00') }}"

    (
        collect_group()
        >> hydra_priority_group(date_str)
        >> [
            k_tt_sweat_indexes(date_str), 
            hydra_group(date_str), 
            sema_group(date_str)
        ]
    )

ecmwf_as_00_dev_taskflow()
