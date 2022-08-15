from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.example_dags.libs.helper import print_stuff
from kubernetes.client import models as k8s

default_args = {
    'owner': 'dykim',
    'depends_on_past': False,
    'start_date': days_ago(0), 
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
}

resource_config = {"KubernetesExecutor": {"request_memory": "200Mi", 
                                          "limit_memory": "200Mi", 
                                          "request_cpu": "200m", 
                                          "limit_cpu": "200m"},
                   "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(labels={"node.genesislab.ai/gpu-node": "true"}))
                }

with DAG(dag_id='std_simple_sample', schedule_interval=None, 
         tags=['analytics'], default_args=default_args) as dag:
    
    task = PythonOperator(
        task_id='std_simple_sample_task',
        python_callable=print_stuff,
        op_args=[20,timetask],
        start_date=days_ago(0),
        owner='airflow',
        executor_config = resource_config
    )

    task