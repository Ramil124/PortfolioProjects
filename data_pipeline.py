from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
import base64
from datetime import timedelta, datetime
import io
import zipfile
import pandas as pd
from sqlalchemy import create_engine
import os
import csv


## Defining variables
OUTPUT_DIR = '/opt/airflow/data'


default_args = {
    'owner': 'r124', 
    'retries':1, 
    'retry_delay':timedelta(minutes=5)
}


def process_zip_data(ti, output_dir):
    """Process zip data: extract the first CSV, use its name, and save it."""
    ## Retrive the base64 encoded content from Xcom
    encoded_content = ti.xcom_pull(task_ids = 'download_zip_file')

    ## Decode the content bask to bytes 
    zip_content = base64.b64decode(encoded_content)

    ## Wrap the bytes content in a BytesIO object 
    zip_content_io = io.BytesIO(zip_content)
    with zipfile.ZipFile(zip_content_io, 'r') as zip_file:
        zip_file.extractall(output_dir)

        csv_name = zip_file.namelist()[0]

    csv_path = os.path.join(output_dir, csv_name)

    ti.xcom_push(key = 'exctracted_csv_path', value = csv_path)


def data_cleaning(ti):
    csv_path = ti.xcom_pull(task_ids ='process_and_save_csv', key = 'exctracted_csv_path' )
    csv_file = pd.read_csv(csv_path, delimiter='|')
## Selecting only required columns to avoid excessive memory issues
    csv_file = csv_file[['BENE_ID', 'CLM_ID', 'PRVDR_NUM', 'CLM_FROM_DT',
                          'CLM_THRU_DT', 'REV_CNTR_TOT_CHRG_AMT', 'REV_CNTR_BENE_PMT_AMT']]
    csv_file['CLM_FROM_DT'] = pd.to_datetime(csv_file['CLM_FROM_DT'])
    csv_file['CLM_THRU_DT'] = pd.to_datetime(csv_file['CLM_THRU_DT'])
    csv_file['REV_CNTR_TOT_CHRG_AMT'] = csv_file['REV_CNTR_TOT_CHRG_AMT'].astype(float)
    csv_file['REV_CNTR_BENE_PMT_AMT'] = csv_file['REV_CNTR_BENE_PMT_AMT'].astype(float)

## Filtering out the negative values
    csv_file = csv_file.loc[
    (csv_file['REV_CNTR_TOT_CHRG_AMT'] > 0) &
    (csv_file['REV_CNTR_BENE_PMT_AMT'] >= 0)]

## Cleaning data out of duplicate values
    csv_file = csv_file.drop_duplicates(subset=['CLM_ID'])

## Calculating percentage of the bill that was actually paid.
    csv_file['PATIENT_RESP_RATIO'] = csv_file['REV_CNTR_BENE_PMT_AMT'] / csv_file['REV_CNTR_TOT_CHRG_AMT']

## Calculating the Service Duration in days
    csv_file['CLAIM_DURATION_DAYS'] = (csv_file['CLM_THRU_DT'] - csv_file['CLM_FROM_DT']).dt.days

## Extracting month and year from a claim date so it is easier to use them during dashboard creation
    csv_file['CLAIM_YEAR'] = csv_file['CLM_FROM_DT'].dt.year
    csv_file['CLAIM_MONTH'] = csv_file['CLM_FROM_DT'].dt.month

    cleaned_path = os.path.join(os.path.dirname(csv_path), 'outpatient_cleaned.csv')

    # Push the path to xcom
    csv_file.to_csv(cleaned_path,sep = ',',quoting= csv.QUOTE_NONE, index=False)

    # Push path to XCom for load task
    ti.xcom_push(key='cleaned_csv_path', value=cleaned_path)



def load_pg(ti):
## Pull the path from XCom
    csv_path = ti.xcom_pull(task_ids ='data_cleaning', key = 'cleaned_csv_path' )
    engine = create_engine("postgresql+psycopg2://healthcare_airflow:healthcare_airflow@postgres/healthcare_airflow")

    first_chunk = True
    for chunk in pd.read_csv(csv_path,delimiter=',', chunksize = 100000 ):
        chunk.to_sql('outpatient_data', con = engine, 
                    if_exists = 'replace' if first_chunk else 'append',
                    index = False)
        first_chunk = False


with DAG(
    dag_id='data_pipeline_dag',
    default_args=default_args,
    description='A data pipeline that downloads a zip file, extracts CSV, and saves to files', 
    schedule = None,
    start_date=datetime(2026,1,25),
    catchup=False
    
        
) as dag:
    download_zip_file = HttpOperator(
        task_id = "download_zip_file",
        method = "GET",
        http_conn_id = 'cms_data', # https://data.cms.gov/ for airflow connections
        endpoint = 'sites/default/files/2023-04/c3d8a962-c6b8-4a59-adb5-f0495cc81fda/Outpatient.zip', 
        response_filter = lambda response: base64.b64encode(response.content).decode('utf-8'),
        log_response = True,
        do_xcom_push = True # Push content to Xcom to retrieve it in the next task
    )

    process_and_save_csv = PythonOperator(
        task_id = 'process_and_save_csv',
        python_callable= process_zip_data,
        op_kwargs={'output_dir': '/opt/airflow/data'},
        do_xcom_push = True
    )

    data_cleaning_task = PythonOperator(
        task_id = 'data_cleaning',
        python_callable= data_cleaning
    )

    load_pg_task = PythonOperator(
        task_id = 'load_pg',
        python_callable= load_pg,
        do_xcom_push = True
        
    )

    download_zip_file >> process_and_save_csv >> data_cleaning_task >> load_pg_task