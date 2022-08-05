from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE,
}

dag = DAG(
    dag_id='kubernetes-dag',
    description='kubernetes pod operator',
    default_args=args,
    max_active_runs=1
)

k = KubernetesPodOperator(
    name="hello-dry-run",
    image="debian",
    cmds=["bash", "-cx"],
    arguments=["echo", "10"],
    labels={"foo": "bar"},
    annotations={"testKey": "testValue1"},
    task_id="dry_run_demo",
    do_xcom_push=True,
)

start = DummyOperator(task_id="start", dag=dag)

start >> k