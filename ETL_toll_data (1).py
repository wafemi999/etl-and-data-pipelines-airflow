from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago


default_args = {

    'owner': 'manuel',
    'start_date': days_ago(0),
    'email': ['emmanoyelola@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

# Define the DAG
dag = DAG(
    dag_id = 'ETL_toll_data',
    default_args = default_args,
    schedule_interval=timedelta(days = 1),
    description = 'Apache Airflow Final Assignment'
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command= 'tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag
)


extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command= 'cut -d "," -f 1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag=dag
)



extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f 5,6,7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag
)


extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='awk \'{print substr($0, 62, 3) "," substr($0, 66, 5)}\' /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag
)



consolidate_data = BashOperator(
    task_id = 'extracted_data.csv',
    bash_command = 'paste -d "," /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag = dag



)


transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk -F "," \'{OFS=","; $4 = toupper($4); print}\' /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
    dag=dag
)
    
#data dependencies 
extract_data_from_csv >> consolidate_data
extract_data_from_tsv >> consolidate_data
extract_data_from_fixed_width >> consolidate_data
unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width]
consolidate_data >> transform_data




