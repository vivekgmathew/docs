import boto3
from operator import itemgetter

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.sensors.s3_key_sensor import S3KeySensor

client = boto3.client('ec2')


def get_latest_ami(**kwargs):
    response = client.describe_images(
        Filters=[
            {
                'Name': 'description',
                'Values': [
                    'Amazon Linux AMI*',
                ]
            },
        ],
        Owners=[
            'amazon'
        ]
    )
    image_details = sorted(response['Images'], key=itemgetter('CreationDate'), reverse=True)
    ami_id = image_details[0]['ImageId']
    return ami_id


def create_instance(**kwargs):
    ti = kwargs['ti']
    ami_id = ti.xcom_pull(task_ids='get_latest_ami')
    response = client.run_instances(
        ImageId=ami_id,
        InstanceType='t2.micro',
        MaxCount=1,
        MinCount=1,
    )



def terminate_instance(**context):
    pass


with DAG(dag_id="cluster_manager",
         schedule_interval="@daily",
         catchup=False) as dag:

    get_latest_ami_id = PythonOperator(
        task_id="get_latest_ami",
        python_callable=get_latest_ami(),
        provide_context=True
    )

    create_instance = PythonOperator(
        task_id="create_instance",
        python_callable=create_instance()
    )

    #create_cluster = EmrCreateJobFlowOperator(
    #    task_id="create_cluster"
    #)

    create_cluster_ctrl_file = PythonOperator(
        task_id="create_cluster_ctrl_file"
    )

    check_recap_ctrl_file = S3KeySensor(
        task_id="check_recap_ctrl_file"
    )

    terminate_instance = PythonOperator(
        task_id="terminate_instance",
        python_callable=terminate_instance()
    )

    #terminate_cluster = EmrTerminateJobFlowOperator(
    #    task_id="terminate_cluster"
    #)
