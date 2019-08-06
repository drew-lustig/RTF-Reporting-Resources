from airflow import DAG
from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import date, timedelta, datetime
import logging
from google.cloud import bigquery as bq
from googleapiclient import discovery
from google.oauth2 import service_account
import pandas as pd
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import re


from dateutil.relativedelta import relativedelta

import os
import sys
from datetime import datetime, timedelta, date
import sys
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO
import numpy as np


#Don't edit any of this, it is good the way it is.

from google.oauth2 import service_account
import json
import base64
from google.cloud import kms_v1
from google.cloud import storage as gcsStorage

SCOPE_TABLE=['rtf.Bing_SEM_Data', 'rtf.Brand_SEM_Data','rtf.BVOS_Data','rtf.GDN_Data','rtf.Google_SEM_Data','rtf.Non_Brand_SEM_Data','rtf.PLA_Data','rtf.Yahoo_Data','rtf.Youtube_Data'] #scope table example is Global_Goals.Consolidate.
PROJECT_ID='essence-analytics-dwh'
sheet_id = "1cefjnQazJxC9gbrPK1IxETedqQDTvPdV4WdsEykZARY"
sheet_tab = ['Data | Bing SEM!A3:L367','Data | Brand SEM!A3:H367','Data | Non-Brand BVOS SEM!A3:H367','Data | GDN!A3:L367','Data | Google SEM!A3:L367','Data | Non-Brand SEM!A3:H367','Data | Yahoo Native!A3:J367','	Data | YouTube!A4:I366']

def fetch_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""
    storage_client = gcsStorage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob_data=blob.download_as_string()
    return blob_data

def decrypt_symmetric(encrypted_text):
    """Decrypts input ciphertext using the provided symmetric CryptoKey."""

    # crypto key params
    project_id='essence-analytics-etl-207615'
    location_id='global'
    key_ring_id='TeamAnalytics'
    crypto_key_id='SecureAnalytics'
    # Creates an API client for the KMS API.
    client = kms_v1.KeyManagementServiceClient()
    response=''
    # The resource name of the CryptoKey.
    name = client.crypto_key_path_path(project_id, location_id, key_ring_id,
                                       crypto_key_id)
    # Use the KMS API to decrypt the data.

    ciphertext=base64.b64decode(encrypted_text)
    response = json.loads(client.decrypt(name, ciphertext).plaintext)
    return response

# Use This function to fetch your credentials. Don't store credentials. Keys could be deleted anytime due to security issue.
# You should have logic to regenrate credentials if you encounter google.auth.exceptions.RefreshError
def getCredentialsFromVault(service_account_email):
    StorageBucket='essence-analytics-5a9706e0-d22d-40f3-abde-2a996137bb0f'
    blob_name=service_account_email+'.json.enc'
    enc_key=fetch_blob(StorageBucket, blob_name)
    credentials = service_account.Credentials.from_service_account_info(decrypt_symmetric(enc_key))
    print(credentials)
    return credentials



def storeDFtoBigQuery(df,table):
    df.rename(columns=lambda x: x.strip(), inplace=True)
    df.rename(columns=lambda x: re.sub('[^a-zA-Z0-9]', '_', x), inplace=True)
    df.rename(columns=lambda x: re.sub('__*', '_', x), inplace=True)
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('-', '')
    df.columns = df.columns.str.replace('.', '')
    df.columns = df.columns.str.replace('/', '_')
    df.columns = df.columns.str.replace('__', '_')

    df.to_gbq(destination_table= table,project_id= PROJECT_ID,if_exists='replace') #fail/replace/append


def readSheetData(service,sheet_id,data_range):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId = sheet_id, range = data_range).execute()
    values = result.get('values', [])
    headers = values.pop(0)
    df = pd.DataFrame.from_records(values, columns = headers)
    return df


def get_together(**kwargs):


	#Scope, this one is good for everything-spreadsheets (reading and writing), google drive and bigquery.

    API_SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
              'https://spreadsheets.google.com/feeds',
              'https://www.googleapis.com/auth/drive',
              'https://www.googleapis.com/auth/cloud-platform']

	#service e-mail.
    service_account_email='data-strategy@essence-analytics-dwh.iam.gserviceaccount.com'

	#On Google sheets, share this e-mail on the google sheets page you want to pull data from.
    credentialsFromVault=getCredentialsFromVault(service_account_email)
    credentialsFromVault = credentialsFromVault.with_scopes(API_SCOPES)
    sheet_service = build('sheets', 'v4', credentials=credentialsFromVault,cache_discovery=False)

    for i in range(len(SCOPE_TABLE)):
        df = readSheetData(sheet_service, sheet_id, sheet_tab[i])
        storeDFtoBigQuery(df,SCOPE_TABLE[i])


# Set DAG default arguments
def_args = {
    'owner': 'Noah Hazan',
    'depends_on_past': False,
    'email': ['noah.hazan@essenceglobal.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'execution_timeout': timedelta(minutes=10),
    'task_concurrency': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG('GFi_Refresh_Dag',
            description='Create',
            default_args =def_args,
            schedule_interval= "@once",
            start_date= datetime(2019, 8, 2), catchup=False)

start_dummy_task = DummyOperator(task_id='start_dummy_task', dag=dag)
end_dummy_task = DummyOperator(task_id='end_dummy_task', dag=dag)
py_operator = PythonOperator(task_id='get_together', python_callable=get_together, dag=dag, provide_context=True)

start_dummy_task >> py_operator >> end_dummy_task
