from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

DEFAULT_DATE = datetime(2016, 1, 1)

test_affinity = {
    'nodeAffinity': {
        'requiredDuringSchedulingIgnoredDuringExecution': {
            'nodeSelectorTerms': [{
                'matchExpressions': [{
                    'key': 'testKey',
                    'operator': 'In',
                    'values': [
                        'testValue1'
                    ]
                }]
            }]
        }
    }
}

args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE,
}

dag = DA11G(
    dag_id='kubernetes-dag',
    description='kubernetes pod operator',
    default_args=args,
    max_active_runs=1
)

k = Kubernet11esPodOperator(
    name="hello-dry-run",
    image="debian",
    cmds=["bash", "-cx"],
    arguments=["echo", "10"],
    labels={"foo": "bar"},
    annotations={"testkey": "testval"},
    task_id="dry_run_demo",
    dag=dag,
    affinity=test_affinity,
)
    