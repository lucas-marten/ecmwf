#import pytest
from airflow.models import DagBag

# Caminho onde os DAGs estão localizados (se necessário, ajuste)
DAG_FOLDER = "/airflow/dags"  # ou o caminho real dos seus DAGs
DAG_ID = "ecmwf_as_00_dev"

#@pytest.fixture
def dagbag():
    """Carrega os DAGs do diretório."""
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)

def test_dag_loaded(dagbag):
    """Verifica se o DAG foi carregado corretamente."""
    dag = dagbag.get_dag("ecmwf_as_00_dev")
    assert dag is not None, "Erro: O DAG não foi carregado!"
    assert dag.dag_id == DAG_ID, "O ID do DAG está incorreto!"

def test_task_count(dagbag):
    """Testa se o DAG tem o número correto de tarefas."""
    dag = dagbag.get_dag("ecmwf_as_00_dev")
    print('nº de tasks: ', len(dag.tasks))
    assert len(dag.tasks) == 4, "O número de tasks no DAG está incorreto!"

def test_task_names(dagbag):
    """Testa se as tasks têm os nomes esperados."""
    dag = dagbag.get_dag("ecmwf_as_00_dev")
    task_ids = {task.task_id for task in dag.tasks}

    expected_task_ids = {"check_S7D", "check_S7S", "download_and_decompress_S7D", "download_and_decompress_S7S"}
    assert task_ids == expected_task_ids, "Os nomes das tasks estão incorretos!"

def test_task_dependencies(dagbag):
    """Verifica se as dependências entre as tasks estão corretas."""
    dag = dagbag.get_dag("ecmwf_as_00_dev")

    check_S7D = dag.get_task("check_S7D")
    check_S7S = dag.get_task("check_S7S")
    download_S7D = dag.get_task("download_and_decompress_S7D")
    download_S7S = dag.get_task("download_and_decompress_S7S")

    # Verifica se cada check precede seu respectivo download
    assert check_S7D.downstream_list == [download_S7D], "Dependência incorreta em check_S7D!"
    assert check_S7S.downstream_list == [download_S7S], "Dependência incorreta em check_S7S!"
