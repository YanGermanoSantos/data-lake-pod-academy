from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime, timedelta

SUBJECT = 'users'

# Configurações do Cluster
JOB_FLOW_OVERRIDES = {
    'Name': f'0001_process_{SUBJECT}_lake',
    'ReleaseLabel': 'emr-7.1.0',
    'Instances': {
        'InstanceGroups': [
            {
                'Name': "MasterNode",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True
    },
    'Applications': [{'Name': 'Spark'}],
    'LogUri': 's3://pod-academy-lake-370943306683/0005_logs/',
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole'
}

# Configurações do Step (Job do Spark)
SPARK_STEPS = [
    {
        'Name': f'process_{SUBJECT}_lake',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--conf', 'spark.log4jHotPatch.enabled=false',
                's3://pod-academy-lake-370943306683/0004_codes/lake/users/users-processing-lake.py'
            ],
        }
    }
]

@dag(
    dag_id='users_process_data_taskflow',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['pod_academy'],
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
)
def emr_users_pipeline():

    # 1. Cria o Cluster EMR
    start_emr_cluster = EmrCreateJobFlowOperator(
        task_id='start_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws',
        region_name='us-east-1'
    )

    # 2. Adiciona o Step (O XCom transitando via .output gera a dependência automática)
    add_step_job = EmrAddStepsOperator(
        task_id='add_step_job',
        job_flow_id=start_emr_cluster.output,
        aws_conn_id='aws',
        steps=SPARK_STEPS,
        region_name='us-east-1'
    )

    # 3. Mini-task para extrair o primeiro ID da lista retornada pelo add_step_job
    @task
    def get_first_step_id(step_ids: list) -> str:
        return step_ids[0]

    step_id_extracted = get_first_step_id(add_step_job.output)

    # 4. Sensor assíncrono para aguardar a finalização do job no Spark
    wait_step_job = EmrStepSensor(
        task_id='wait_step_job',
        job_flow_id=start_emr_cluster.output,
        step_id=step_id_extracted,
        aws_conn_id='aws',
        poke_interval=10,
        timeout=60 * 30, # 30 minutos
        mode='reschedule', # Libera o worker durante a espera
        region_name='us-east-1'
    )

    # 5. Encerra o cluster EMR (Garante a execução mesmo se houver falha no Sensor)
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id=start_emr_cluster.output,
        aws_conn_id='aws',
        trigger_rule='all_done',
        region_name='us-east-1'
    )

    # Ordem de execução explícita apenas para garantir o encerramento no final
    wait_step_job >> terminate_emr_cluster

# Invoca a função para o Airflow registrar a DAG
emr_users_pipeline()