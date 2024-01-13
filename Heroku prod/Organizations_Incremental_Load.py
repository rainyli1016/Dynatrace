from zenpy import Zenpy
import zenpy
import requests
import pandas as pd
import datetime as dt
from datetime import timezone
import json, sys
import os
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from io import StringIO
import io
import csv
import numpy as np
import base64
from pandas.api.types import infer_dtype
import smtplib
gmail_user = 'bi.dynatrace@gmail.com'
gmail_password = 'biteamisthebest!'
s = smtplib.SMTP('smtp.gmail.com', 587)
s.starttls()
FROM = 'bi.dynatrace@gmail.com'
TO = ["rainy.li@dynatrace.com"]
SUBJECT = "Zendesk Organizations Incremental Load Error (Prod)!"
try:
    host = "ec2-54-161-114-24.compute-1.amazonaws.com"
    dbname = "d5lblgv4vjgptk"
    port = 5432
    user = "int_zendesk_rw"
    password = "p380b89939880318d091085ed2fff9af1d3206575dd74e152eed2027542e86532"
    sslmode = "require"
    postgres_str = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    conn = psycopg2.connect(postgres_str)
    cnx = create_engine(postgres_str)
    conn.autocommit = True
    cur = conn.cursor()
except:
    TEXT = "Hi BI Team, Zendesk Organization Incremental Load Failed - Database Connection Issue need to be fixed."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
else:
    job_name = 'Organizations_Incremental_Load'
# Insert new row to job summary log table
    cur.execute("insert into logging.job_summary_logs(job_name,start_time,end_time,number_of_records,status,message) values ('"+ job_name +"', current_timestamp, NULL, NULL,'In Progress',NULL)")
    conn.commit()
    sql_query = "select max(job_execution_id) from logging.job_summary_logs where job_name = '"+job_name+"'"
    cur.execute(sql_query)
    result = cur.fetchone()
    job_exec_id = str(result[0])
# Insert new row to job details log table
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'database connection', current_timestamp, NULL, 'Success', NULL)")
    conn.commit()
# Call Zendesk API
try:
    creds = {
    'email' : '<bussys_data_processing@dynatrace.com>',
    'token' : 'toWh50uAmq3KaO8idoDBVaPvV3sOZPCYaA1N6pwd',
    'subdomain': 'dynatrace'
    }
    zenpy_client = Zenpy(**creds)
except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'api connection', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Organization Incremental Load Failed - Zendesk API Connection Issue need to be fixed."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
else:
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'api connection', current_timestamp, NULL, 'Success', NULL)")
    conn.commit()

try:
    sql_query = "select max(updated_at) from int_zendesk.zendesk_organizations"
    cur.execute(sql_query)
    result = cur.fetchone()
    max_updated_timestamp = result[0]
    sql_query = "select EXTRACT(EPOCH FROM TIMESTAMP WITH time zone '"+max_updated_timestamp+"') "
    cur.execute(sql_query)
    result = cur.fetchone()
    max_updated_epoch = int(result[0])
    orgs_df = pd.DataFrame()
    stream_end = False
    while stream_end == False:
        result_generator = zenpy_client.organizations.incremental(start_time=max_updated_epoch)
        df = pd.DataFrame()
        for org in result_generator:
            orgs_json = org.to_json()
            df = pd.json_normalize(json.loads(orgs_json))
        #print(df)
            orgs_df = pd.concat([orgs_df, df],ignore_index=True)
        stream_end = result_generator.end_of_stream
        if stream_end == True:
            break
except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'fetch data from api', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Organization Incremental Load Failed - Fetch data from API process need to be fixed."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit()
else:
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'fetch data from api', current_timestamp,"+ str(len(orgs_df)) +", 'Success', NULL)")
    conn.commit()

try:
    df = orgs_df
    for col in df.columns:
        if infer_dtype ( df[col] ) == 'mixed':
        # ‘mixed’ is the catchall for anything that is not otherwise specialized
            df[col] = df[col].astype ( 'str' )
    df['domain_names'] = df['domain_names'].map(lambda x: str(x).lstrip('[').rstrip(']')).astype(str)
    df['tags'] = df['tags'].map(lambda x: str(x).lstrip('[').rstrip(']')).astype(str)
    df['date_inserted'] = dt.datetime.now()
    ##df['organization_fields.dynatrace_one_csm'] = df['organization_fields.dynatrace_one_csm'].mask (
    ##df['organization_fields.dynatrace_one_csm'].notnull (), np.nan )
    df['organization_fields.dynatrace_one_csm'] = df['organization_fields.dynatrace_one_csm'].replace ( np.nan, 'no_email', regex=True )
    df['organization_fields.dynatrace_one_csm'] = df['organization_fields.dynatrace_one_csm'].str.encode ( 'utf-8', 'strict' ).apply ( base64.b64encode )
    df['organization_fields.dynatrace_one_csm'] = df['organization_fields.dynatrace_one_csm'].astype ( str )
    df['organization_fields.dynatrace_one_csm'] = df['organization_fields.dynatrace_one_csm'].replace ( "b'", '', regex=True )
    df['organization_fields.dynatrace_one_csm'] = df['organization_fields.dynatrace_one_csm'].replace ( "'", '', regex=True )
    df.replace('', np.nan, inplace=True)
    ###df2[col] = df2[col].map(lambda x: str(x).lstrip('[').rstrip(']')).astype(str)
except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'data cleanup and manipulation', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Organization Incremental Load Failed - Data cleaning process need to be fixed, try to check data type in the dataframe. "
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit()
else:
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'data cleanup and manipulation', current_timestamp, NULL, 'Success', NULL)")
    conn.commit()

try:
    df.to_sql('zendesk_organizations', cnx, schema = 'int_zendesk', method='multi', if_exists='append',index=False)
except:
    cur.execute ("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id )
    cur.execute ("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'load data to database', current_timestamp, NULL, 'Failed', NULL)" )
    conn.commit ()
    TEXT = "Hi BI Team, Zendesk Organization Incremental Load Failed - Loading data to database process need to be fixed, try to check data type."
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit()
else:
    cur.execute ("update logging.job_summary_logs set status ='Success', end_time = current_timestamp, number_of_records = " + str (len ( df ) ) + "  where job_execution_id = " + job_exec_id )
    cur.execute ("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'load data to database', current_timestamp," + str (len ( df ) ) + ", 'Success', NULL)" )
    conn.commit ()

conn.close ()
cnx.dispose ()