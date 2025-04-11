def test_dag():
    # from airflow.models import DagBag
    # dag = DagBag(include_examples=False, dag_folder="/airflow/dags/").get_dag("ecmwfe_as_12")
    import sys

    sys.path.insert(1, "/airflow/dags")
    from ecmwf_dev.ecmwfe_as_12 import ecmwfe_as_12_dev

    dag = ecmwfe_as_12_dev()
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
    # dag = DagBag(include_examples=False, dag_folder="/airflow/dags/").get_dag("ecmwfe_as_12")
    import sys

    sys.path.insert(1, "/airflow/dags")
    from ecmwf_dev.ecmwfe_as_12 import ecmwfe_as_12_dev

    dag = ecmwfe_as_12_dev()
    groups = {
        "collect_group": [
            "check_s3_CLE",
            "download_decompression_CLE",
            "check_download_CLE"
        ],
        "hydra_priority": ["msg2nc", "mergetime"],
        "hydra": [
            "hydra_height_100m",
            "hydra_joules_radiation_convert",
            "hydra_calc",
            "hydra_ens_pctl"
        ]
    }

    for group, downstream_list in groups.items():
        for task in downstream_list:
            task = f"{group}.{task}"
            assert dag.has_task(task), "A task não existe."
            assert (
                dag.get_task(task).task_group.group_id == group
            ), "A task não pertence ao grupo correto."
