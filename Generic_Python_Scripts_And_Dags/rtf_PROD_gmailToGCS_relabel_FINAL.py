import base64
from email.mime.text import MIMEText
from io import BytesIO
import json
import logging
import os
from datetime import datetime

from airflow import DAG, models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from apiclient import errors
from essence.analytics.platform import securedcredentials as secure_creds
from google.cloud import storage
from google.oauth2.credentials import Credentials
from googleapiclient import discovery
import pandas as pd

SCOPES = ['https://mail.google.com/'] # grants all permissions. proceed with caution.

API_NAME = 'gmail'
API_VERSION = 'v1'
userId='nbcu_analytics_data@essenceglobal.com'


def query_for_message_ids(service, search_query):

    result = service.users().messages().list(userId='me', q=search_query).execute() # Search for messages matching query
    results = result.get('messages')
    if results:
        msg_ids = [r['id'] for r in results] # Put matching messages' IDs into a list
    else:
        msg_ids = []

    return msg_ids

def modify_message(gmail_service, userId, msg_id, msg_labels):

    message = gmail_service.users().messages().modify(userId=userId, id=msg_id, body=msg_labels).execute()

    label_ids = message['labelIds']
    print(label_ids)

    print('Message ID: %s - With Label IDs %s' % (msg_id, label_ids))


def create_msg_labels():
  """Create object to update labels.

  Returns:
    A label update object.
  """
  return {'removeLabelIds': [], 'addLabelIds': ['UNREAD', 'INBOX', 'Label_2']}

def get_attachments(service, user_id, msg_id, message=None):

    if not message:
        message = service.users().messages().get(userId=user_id, id=msg_id).execute()

    atts = {}
    for part in message['payload']['parts']:
        if part['filename'] and part['mimeType'] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            if 'data' in part['body']:
                data = part['body']['data']
            else:
                att_id = part['body']
                att = service.users().messages().attachments().get(userId=user_id, messageId=msg_id, id=att_id).execute()
                data = att['data']
            atts[part['filename']] = BytesIO(base64.urlsafe_b64decode(data.encode('utf-8')))
    
    return atts

def process_file(file):
    # TODO
    tabs = pd.read_excel(file, sheet_name=['Video','Display'], skiprows=2).iloc[:, 1:]
    errors = None
    return errors

def send_reply(service, thread_id, headers, filename, errors):
    
    if errors:
        message_text = "something" # TODO
    else:
        message_text = "something else" # TODO

    message = MIMEText(message_text)
    message['to'] = headers['From']
    message['from'] = 'nbcu_analytics_data@essenceglobal.com'
    message['subject'] = headers['Subject']
    body = {'raw': base64.urlsafe_b64encode(message.as_string())}
    service.users().messages().send(userId='me', body=body).execute()


def processEmailScanning():

    data_value = secure_creds.getDataFromEssenceVault(userId) # Gets creds from vault
    credentials_dict = json.loads(data_value) # Loads creds as dict
    credentials = Credentials(**credentials_dict) # Creates Google credentials from creds dict
    gmail_service = discovery.build(API_NAME, API_VERSION, credentials=credentials, cache_discovery=False) # Build Gmail API service
    message_ids = query_for_message_ids(gmail_service, 'has:attachment')

    for msg_id in message_ids:
        message = gmail_service.users().messages().get(userId='me', id=msg_id).execute()
        thread_id = message['threadId']
        payload = message['payload']
        headers = {header['name']: header['value'] for header in payload['headers']}
        attachments = get_attachments(gmail_service, 'me', msg_id, message=message)
        for filename, file in attachments.items():
            errors = process_file(file)
            send_reply(gmail_service, thread_id, headers, filename, errors)      

    # for i in range(len(messageID)):
    #     get_attachments(gmail_service, 'me', messageID[i])
    #     message = gmail_service.users().messages().modify(userId=userId, id=messageID[i], body={'removeLabelIds': [], 'addLabelIds': ['Label_7576572066746469364']}).execute()

dag = DAG(
    'rtf_PROD_gmailToGCS_relabel_FINAL',
    description='DAG to fetch gmail attachments and label as Processed',
    schedule_interval=None,
    start_date=datetime(2019, 8, 24),
    catchup=False
)

dummy_operator = DummyOperator(task_id='dummy_task', retries=1, dag=dag)

py_operator = PythonOperator(task_id='fetchDataFromGmail',
    python_callable=processEmailScanning, dag=dag)

dummy_operator >> py_operator
