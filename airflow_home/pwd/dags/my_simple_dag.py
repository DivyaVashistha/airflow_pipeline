import datetime as dt
import os
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account

log = ""
UPLOAD_DIRECTORY = "./my_pipeline"
UPLOAD_DIRECTORY = os.path.abspath(UPLOAD_DIRECTORY)


def extract():
    """
    Fetch code from api for latest coronavirus data.
    :return: str, date , date for which the latest data has been collected.
    :return: int, number of rows stored in latest csv file in local system.
    """
    url = "https://api.covid19india.org/data.json"
    JSONContent = requests.get(url).json()

    my_list = []

    for i in JSONContent['statewise']:
        d = {'state': i['state'], 'count': i['confirmed'], 'date': datetime.now().date()}
        my_list.append(d)

    df = pd.DataFrame(data=my_list)
    # print(df.head(10))
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d").dt.date
    # print(df.head(10))
    df.to_csv(UPLOAD_DIRECTORY + '/reports/{}.csv'.format(datetime.now().date()), index=False)
    print("data added to csv with {} rows on {} at {}".format(len(df), datetime.now().date(), datetime.now().time()))
    global log
    log = "data added to csv with {} rows on {} at {}".format(len(df), datetime.now().date(), datetime.now().time())
    write_log()
    return datetime.now().date(), len(df) - 1


def write_log():
    """
    Method to write logs to file on local system.
    :return: None
    """
    log_list = [{'log': log}]
    df = pd.DataFrame(log_list)
    df.to_csv(UPLOAD_DIRECTORY + '/logs.csv', mode='a', header=False)


def load_data(file_date):
    """
    Method to load(append) data to big query partitioned table.
    :param file_date: str , name of file by date which has to be loaded
    :return: int , total rows uploaded into big query table
    """
    key_path = UPLOAD_DIRECTORY + "/My Project-3b4aa17f4859.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.schema_update_options = ['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION']
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.skip_leading_rows = 2
    # job_config.autodetect = True

    # creating a reference for the dataset in which the table is present
    dataset_ref = client.dataset('dataset_airflow_etl')

    print(file_date)
    date = str(file_date).replace('-', '')
    # creating a reference to the table
    table_ref = dataset_ref.table('airflow_table_assignment_divya${}'.format(date))

    filename = UPLOAD_DIRECTORY + '/reports/{}.csv'.format(file_date)
    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.

    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_ref, table_ref))
    return job.output_rows


def extract_report_func(**kwargs):
    """
    Method used by python operator to execute extraction for the pipeline.
    :param kwargs: dictionary , stores data flowing between operators in airflow
    :return: str , name of the latest extracted file
    """
    report_name = extract()
    return report_name


def load_data_func(**kwargs):
    """
    Method used by python operator to execute loading of data for the pipeline.
    :param kwargs: dictionary , stores data flowing between operators in airflow
    :return: int , total number of rows returned from load data method
    """
    ti = kwargs['ti']
    report_name, rows = ti.xcom_pull(task_ids='extract')
    output_rows = load_data(report_name)
    return output_rows


def get_percentage_func(**kwargs):
    """
    Method used by python operator to execute assignment calculation for the pipeline.
    :param kwargs: dictionary , stores data flowing between operators in airflow
    :return: double , percentage of successfully loaded data for the pipeline
    """
    ti = kwargs['ti']
    output_rows = ti.xcom_pull(task_ids='load')
    report_name, rows = ti.xcom_pull(task_ids='extract')
    return (output_rows * 100) / rows


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 6, 2, 7, 37, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('my_simple_dag',
         default_args=default_args,
         schedule_interval='*/2 * * * *',
         ) as dag:
    opr_hello = BashOperator(task_id='say_Hi',
                             bash_command='echo "Hi!!"')

    opr_extract = PythonOperator(task_id='extract',
                                 python_callable=extract_report_func,
                                 provide_context=True)

    opr_load = PythonOperator(task_id='load',
                              python_callable=load_data_func,
                              provide_context=True)

    opr_calc = PythonOperator(task_id='calc',
                              python_callable=get_percentage_func,
                              provide_context=True)

opr_hello >> opr_extract >> opr_load >> opr_calc
