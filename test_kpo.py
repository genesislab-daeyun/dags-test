import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount

share_volume = Volume(
    name='share',
    configs={'hostPath': {'path': '/data-cephfs/mlops', 'type': 'Directory'}},
)
share_volume_mount = VolumeMount(
    name='share',
    mount_path='/opt/tmp',
    sub_path=None,
    read_only=False
)

with DAG(
    dag_id='02_unscheduled_kube',
    start_date=datetime.now(),
    schedule_interval=None,
) as dag:
    make_dataframe = KubernetesPodOperator(
        task_id='make_dataframe',
        image='110.45.155.232:8080/mlops-test/mlops-make_dataframe:latest',
        name='make_dataframe',
        namespace='mlops',
        volumes=[share_volume],
        volume_mounts=[share_volume_mount],
        cmds=['python3.10'],
        arguments=['--output_path', '/opt/tmp/stat.csv'],
        get_logs=True,
        do_xcom_push=False,
    )

    calculate_stat = KubernetesPodOperator(
        task_id='calculate_stat',
        image='110.45.155.232:8080/mlops-test/mlops-calculate_stat:latest',
        image_pull_secrets=[k8s.V1LocalObjectReference("harbor")],
        name='make_dataframe',
        namespace='mlops',
        volumes=[share_volume],
        volume_mounts=[share_volume_mount],
        cmds=['python3.10'],
        arguments=[
            '--input_path', '/opt/tmp/stat.csv',
            '--output_path', '/opt/tmp/result.csv'
        ],
        get_logs=True,
        do_xcom_push=False,
    )

    make_dataframe >> calculate_stat