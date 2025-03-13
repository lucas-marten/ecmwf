import sys


def test_dag_structure():
    sys.path.insert(1, "/airflow/dags")
    from ecmwf_dev.ecmwf_as_12 import ecmwf_as_12_dev_taskflow

    dag = ecmwf_as_12_dev_taskflow()
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


def test_groups():
    sys.path.insert(1, "/airflow/dags")
    from ecmwf_dev.ecmwf_as_12 import ecmwf_as_12_dev_taskflow

    dag = ecmwf_as_12_dev_taskflow()
    task_groups = dag.task_group.get_task_group_dict()

    assert task_groups["collect_group"], "collect_group não existe."
    assert task_groups["hydra_priority_group"], "hydra_priority_group não existe."
    assert task_groups["k_tt_sweat_indexes"], "k_tt_sweat_indexes não existe."
    assert task_groups["hydra_group"], "hydra_group não existe."
