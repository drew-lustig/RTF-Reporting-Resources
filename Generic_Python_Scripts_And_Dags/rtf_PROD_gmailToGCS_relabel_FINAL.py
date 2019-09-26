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
from airflow.operators.subdag_operator import SubDagOperator
from apiclient import errors
from essence.analytics.platform import securedcredentials as secure_creds
from google.cloud import storage
from google.oauth2.credentials import Credentials
from googleapiclient import discovery
import pandas as pd
from xlrd.biffh import XLRDError

SCOPES = ['https://mail.google.com/'] # grants all permissions. proceed with caution.

API_NAME = 'gmail'
API_VERSION = 'v1'
userId='nbcu_analytics_data@essenceglobal.com'

credentials = get_google_creds()
gmail_service = create_gmail_service()

def get_google_creds():
    data_value = secure_creds.getDataFromEssenceVault(userId)
    credentials_dict = json.loads(data_value)
    credentials = Credentials(**credentials_dict)
    return credentials

def create_gmail_service():
    gmail_service = discovery.build(API_NAME, API_VERSION, credentials=credentials, cache_discovery=False)
    return gmail_service 

def create_bq_client():
    bq_client = bq.Client(credentials=credentials, project='essence-analytics-nbcu-dwh')
    return bq_client

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

def test_video(df: pd.DataFrame):

    errors = []
    video_columns = {'Site': 'site', 'Data Source': 'dataSource', 'Date': 'date', 'DCM Package Name': 'placementGroupName',
                    'DCM Placement Name or ID': 'placementName', 'Creative': 'creativeName', 'Skip/Non-Skip': 'skip',
                    'Video Type': 'videoType', 'Impressions (if applicable)': 'impressions', 'Video Starts': 'videoPlays',
                    '25% Completion': 'videoFirstQuartileCompletions', '50% Completion': 'videoMidpoints',
                    '75% Completion': 'videoThirdQuartileCompletions', 'Video Completes': 'videoCompletions'}
    missing_cols = [lambda x: x for x in video_columns if x not in df.columns] # Columns that are missing from the file
    if len(missing_cols) > 0:
        errors.append("File is missing these columns: " + ", ".join(missing_cols))

    df.drop(df.loc[df.Site == 'Ex'].index, inplace=True) # Get rid of example row

    if df.Site.count() not in [1, df.shape[0]]: # Check site column
        errors.append("Unable to determine Site for some rows. If using merged cells, try un-merging them.")
    elif df.Site.count() == 1:
        df['Site'] = df.loc[~df['Site'].isna()]['Site'].values[0]
        
    try:
        pd.to_datetime(df.Date) # Check date column
    except ValueError:
        errors.append("At least one invalid date in Date column.")

    number_cols = [col for col in [
        'Impressions (if applicable)', 'Video Starts', '25% Completion',
        '50% Completion', '75% Completion', 'Video Completes']
        if col not in missing_cols
    ]
    error_num_cols = []
    for col in number_cols: # Check number columns
        try:
            pd.to_numeric(df[col])
        except ValueError:
            error_num_cols.append(col)
    if error_num_cols > 0:
        msg = "Non-numeric data in the following columns: " + ", ".join(error_num_cols)
        errors.append(msg)

    if 'Video Completes' not in missing_cols: # Check video completes/starts
        if not df.loc[~df['Video Starts'].isna() & df['Video Completes'].isna()].empty:
            msg = "Some rows have Video Starts but no Video Completes."
            errors.append(msg)
        if not df.loc[df['Video Starts'] < df['Video Completes']].empty:
            msg = "Some rows have more Video Completes than Video Starts."
            errors.append(msg)

    dimension_cols = [col for col in video_columns if (col not in num_cols) & (col not in missing_cols)]
    check_dups = df.groupby(dimension_cols).count().max(axis=1)
    if not check_dups.loc[check_dups > 1].empty:
        msg = "Duplicate rows found."
        errors.append(msg)
        
    return errors

def test_display(df: pd.DataFrame):
    # TODO
    pass

def test_file(file):

    errors = []
    try:
        video = pd.read_excel(file, sheet_name='Video', skiprows=2, usecols=list(range(1, 16)))
        errors.extend(test_video(video))
        display = pd.read_excel(file, sheet_name='Display', skiprows=2, usecols=list(range(1, 6)))
        errors.extend(test_display(display))
    except XLRDError as e:
        errors.append(e.__str__())

    return errors

def create_reply(thread_id, headers, body_text=None):
    message = MIMEText(body_text)
    message['To'] = headers['From']
    message['From'] = 'nbcu_analytics_data@essenceglobal.com' # TODO: Change
    message['Subject'] = headers['Subject']
    body = {'raw': base64.urlsafe_b64encode(message.as_string())}
    return body

def get_messages():
    message_ids = query_for_message_ids(gmail_service, 'has:attachment')
    return message_ids

def send_reply(message, file_results):
    thread_id = message['threadId']
    payload = message['payload']
    headers = {header['name']: header['value'] for header in payload['headers']}
    bodies = []
    for filename, results in file_results.items():
        if results == "Success": # TODO: Change
            msg = filename + ": Successfully processed."
        else:
            msg = filename + ": \n" + "\n".join(results)
        bodies.append(msg)
    text = ("Thank you for sending data to nbcu_analytics_data.\n"
            "Please review any errors below and send new files if necessary:\n"
            + "\n".join(bodies)
            + "\n\nThis inbox is not actively monitored. If you have questions, "
            "please contact the NBCU planning team for this campaign."
    )
    body = create_reply(thread_id, headers, body_text=text)
    gmail_service.users().messages().send(userId='me', body=body).execute()

def file_to_bq(file):

    # Create BQ Client
    # Upload to temp table
    # Merge? Maybe do this after human check

def process_emails(**context):
    ti = context['ti']
    msg_ids = ti.xcom_pull(task_ids='get_messages')
    for id in msg_ids:
        results = {}
        message = gmail_service.user().messages().get(userId='me', id=id).execute()
        attachments = get_attachments(gmail_service, 'me', id, message=message)
        for filename, file in attachments.items():
            file_results = test_file(file)
            if len(file_results) == 0:
                try:
                    file_to_bq(file) # TODO
                except Exception as e: # TODO
                    file_results.append(e.__str__())
            results[filename] = file_results
            send_reply(message, results)

dag = DAG(
    'siteServed_to_BQ',
    description='DAG to fetch gmail attachments and push to BQ',
    schedule_interval=None,
    start_date=datetime(2019, 8, 24),
    catchup=False
)

dummy_operator = DummyOperator(task_id='dummy_task', retries=1, dag=dag)

getMessages = PythonOperator(task_id='getMessages', python_callable=get_messages, dag=dag)
processEmails = PythonOperator(
    task_id='processEmails',
    python_callable=process_emails,
    provide_context=True,
    dag=dag)

dummy_operator >> py_operator
