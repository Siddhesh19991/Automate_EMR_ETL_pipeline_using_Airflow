from airflow import DAG
from datetime import timedelta, datetime 
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
import json 


#Setting up the EMR 
job_flow_overrides = {
    "Name": "de_emr_cluster",
    "ReleaseLabel": "emr-7.2.0",
    "Applications": [{"Name": "Spark"}],
    "LogUri": "s3://auto-de-data/emr-logs/",
    "VisibleToAllUsers":False, #only the root and cluster creater can perform EMR actions
    "Instances": { #setting up 1 master node and 2 core nodes 
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
         
         #created a VPC with 2 public subset 
        "Ec2SubnetId": "subnet-0c8629a30644bace2", # one of the subset
        "Ec2KeyName" : 'weather-api-keypair',
        "KeepJobFlowAliveWhenNoSteps": True, #to allow the cluster to not shut down after a step completes 
        "TerminationProtected": False, # Setting this as false will allow us to programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole", #role for
    "ServiceRole": "EMR_DefaultRole",
}

default_args = {
    'owner': 'de_airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 29), #year-month-day
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


SPARK_EXTRACT = [
    {
        "Name": "Extract data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "s3://ap-southeast-2.elasticmapreduce/libs/script-runner/script-runner.jar", #To run scripts saved on S3 on the cluster (ensure the right region is set)
            "Args": [
                "s3://auto-de-data/scripts/ingest.sh", #the script that needs to be run that is store on S3 (wget command)
                #to run via CL
            ],
        },
    },#can add more steps that does something else inside this. 
   ]

SPARK_TRANSFORM = [
    {
        "Name": "Transform data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar", 
            "Args": ["spark-submit", #needed since we are we are submitting a spark job via PySpark
                "s3://auto-de-data/scripts/transform.py", #the python script that needs to be run that is store on S3 
                #to run via CL
            ],
        },
    },#can add more steps that does something else inside this. 
   ]


with DAG("DE_dag",
         default_args=default_args,
         catchup=False,
         schedule_interval="@daily") as dag:

        start_workflow = DummyOperator(task_id = "start_workflow") # Doesnt do anything but just a placeholder

        emr_initialize = EmrCreateJobFlowOperator(
            task_id  = "emr_initialize",
            job_flow_overrides=job_flow_overrides
            # aws_conn_id="aws_default" # not reqquired if "aws configure" is setup 
        )

        emr_active_check = EmrJobFlowSensor(
            task_id = "emr_active_check",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='emr_initialize', key='return_value') }}", #xcom value from airflow to grab the cluster ID of the EMR created
            target_states={"WAITING"},  # Specify the desired state i.e will output success if the EMR is ready. (Should always be in uppercase)
            timeout=3600, #timeout after 3600 seconds
            poke_interval=5, #will then poke every 5 seconds
            mode='poke',
        )

        # extract the data 
        extract_data = EmrAddStepsOperator(
            task_id="extract_data",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='emr_initialize', key='return_value') }}", #which EMR cluster it has to run on
            steps= SPARK_EXTRACT
        )

        #to check if the extraction was a success
        extract_check = EmrStepSensor( 
            task_id = "extract_check",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='emr_initialize', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='extract_data')[0] }}", #[0] since we have only 1 step inside "SPARK_EXTRACT"
            target_states={"COMPLETED"}, 
            timeout=3600,
            poke_interval=5,
        )

        #transform the data

        transform_data = EmrAddStepsOperator(
            task_id="transform_data",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='emr_initialize', key='return_value') }}", #which EMR cluster it has to run on
            steps= SPARK_TRANSFORM
        )


        #to check if the transformation was a success
        transform_check = EmrStepSensor( 
            task_id = "transform_check",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='emr_initialize', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='transform_data')[0] }}", #[0] since we have only 1 step inside "SPARK_TRANSFORM"
            target_states={"COMPLETED"}, 
            timeout=3600,
            poke_interval=5,
        )

        #Terminate the cluster 
        cluster_terminate = EmrTerminateJobFlowOperator(
            task_id = "cluster_terminate",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='emr_initialize', key='return_value') }}"
        )

        #check if termination was a success
        terminate_check = EmrJobFlowSensor(
            task_id = "emr_terminate_check",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='emr_initialize', key='return_value') }}", #xcom value from airflow to grab the cluster ID of the EMR created
            target_states={"TERMINATED"},  # Specify the desired state i.e will output success if the EMR is ready. (Should always be in uppercase)
            timeout=3600, #timeout after 3600 seconds
            poke_interval=5, #will then poke every 5 seconds
            mode='poke',
        )

        end_workflow = DummyOperator(task_id = "end_workflow") 


        start_workflow >> emr_initialize >> emr_active_check >> extract_data >> extract_check >> transform_data >> transform_check >> cluster_terminate >> terminate_check >> end_workflow