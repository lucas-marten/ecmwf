def test_dag():
    # from airflow.models import DagBag
    # dag = DagBag(include_examples=False, dag_folder="/airflow/dags/").get_dag("ecmwf_as_12")
    import sys

    sys.path.insert(1, "/airflow/dags")
    from ecmwf_dev.ecmwf_as_12 import ecmwf_as_12

    dag = ecmwf_as_12()
    assert dag.tags, "A DAG não contém tags."
    assert dag.doc_md, "A DAG não contém documentação."
    # assert dag.catchup, "A DAG não está configurada para catchup."
    assert dag.schedule_interval, "A DAG não contém schedule."
    assert dag.default_args, "A DAG não contém default_args."
    assert dag.dag_id, "A DAG não contém dag_id."
    assert dag.description, "A DAG não contém description."
    assert dag.max_active_runs, "A DAG não contém max_active_runs."
    assert dag.max_active_tasks, "A DAG não contém max_active_tasks."
    assert dag.owner, "A DAG não contém owner."
    assert dag.start_date, "A DAG não contém start_date."


def test_tasks():
    # from airflow.models import DagBag
    # dag = DagBag(include_examples=False, dag_folder="/airflow/dags/").get_dag("ecmwf_as_12")
    import sys

    sys.path.insert(1, "/airflow/dags")
    from ecmwf_dev.ecmwf_as_12 import ecmwf_as_12

    dag = ecmwf_as_12()
    groups = {
        "collect_group": [
            "check_s3_S7D",
            "download_decompression_S7D",
            "check_download_S7D",
            "check_s3_S7S",
            "download_decompression_S7S",
            "check_download_S7S",
        ],
        "hydra_priority_group": ["msg2nc", "mergetime"],
        "hydra_group": [
            "rain_rate",
            "hydra_joules_radiation_convert",
            "calc_10m_wind_speed",
        ],
        "hydra_group.hydra_secondarys_group": [
            "msg2nc_others",
            "mergetime_others",
            "msg2nc_cloud",
            "mergetime_cloud",
            "msg2nc_level",
            "mergetime_level",
            "msg2nc_height",
            "mergetime_height",
        ],
        "hydra_group.hydra_calc_group": ["hydra_leaf_wetting_convert"],
        "hydra_group.hydra_wind_group": ["calc_alpha", "hydra_100m_direction", "hydra_100m_speed", "hydra_100m_gust"],
    }

    for group, downstream_list in groups.items():
        for task in downstream_list:
            task = f"{group}.{task}"
            assert dag.has_task(task), "A task não existe."
            assert (
                dag.get_task(task).task_group.group_id == group
            ), "A task não pertence ao grupo correto."
