#IMPORT LIBRARIES

from datetime import timedelta

from airflow import DAG 

from airflow.operators.bash_operator import BashOperator

from airflow.utils.dates import days_ago

#DEFINE DAG ARGUMENTS -----------

default_args = {
    'owner' : 'Wazir Rohiman', #owner of the dag
    'start_date' : days_ago(0), #when does the dag start running - this means today
    'email' : ['wazir.rohiman@somemail.com'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1, #number of retry if dag fails
    'retry_delay' : timedelta(minutes=5), #retry after every 5 minutes
}


# DEFINE THE DAG ----------

dag = DAG(
    'ETL_toll_data', #dag name
    default_args=default_args, #pass the dag arguments (settings) in the DAG object
    description='Apache Airflow Final Assignment', #explain what the dag does
    schedule_interval=timedelta(days=1), #how often should the dag run - in this case daily
)

# DEFINING THE TASKS -------------

# Task 1: Unzip data

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='sudo tar -zxvf /home/project/airflow/dags/finalassignment/staging/tolldata.tgz',
    dag=dag,
)


# Task 2: Extract data from CSV

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='sudo cut -d"," -f1-4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    dag=dag,
)


# Task 3: Extract data from TSV

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='sudo cut -f1-7 tollplaza-data.tsv | tr -d "\r" > clean_tollplaza-data.tsv ; cut -f5-7 /home/project/airflow/dags/finalassignment/staging/clean_tollplaza-data.tsv  | tr "\t" "," > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv',
    dag=dag,
)


# Task 4: Extract data from fixed width file

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -f6-7 payment-data.txt | tr " " "," > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv',
    dag=dag,
)

#Set explicit dependencies
extract_data_from_fixed_width.set_upstream(unzip_data)
extract_data_from_fixed_width.set_upstream(extract_data_from_csv)
extract_data_from_fixed_width.set_upstream(extract_data_from_tsv)

# Task 5: Consolidate data

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='sudo paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)


#Task 6: Transform data

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='sudo awk -F "," "{$4=toupper($4);print}" OFS="," extracted_data.csv > transformed_data.csv',
    dag=dag,
)

# DEFINING TASK PIPELINE ---------------

#Task pipeline

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data


