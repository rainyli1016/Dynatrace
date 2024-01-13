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
from pandas.api.types import infer_dtype
import logging, logging.handlers
import traceback
import smtplib
from sqlalchemy import (Column, DateTime, Integer, MetaData,
                        String, Table, create_engine, text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper, sessionmaker
import time
import time
import base64
gmail_user = 'bi.dynatrace@gmail.com'
gmail_password = 'biteamisthebest!'
s = smtplib.SMTP('smtp.gmail.com', 587)
s.starttls()
FROM = 'bi.dynatrace@gmail.com'
TO = ["rainy.li@dynatrace.com"]
SUBJECT = "Zendesk User incremental Load Base64 Error (Prod)!"
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
    TEXT = "Hi BI Team, Zendesk Users Incremental Load Base64 Failed- Database Connection Issue need to be fixed."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
else:
    job_name = 'Users_Incremental_Load_Base64'
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

#https://dynatrace.zendesk.com/api/v2/users.json
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
    TEXT = "Hi BI Team, Zendesk Users Incremental Load Base64 Failed- Zendesk API Connection Issue need to be fixed."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit()
else:
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'api connection', current_timestamp, NULL, 'Success', NULL)")
    conn.commit()

try:
    sql_query = "select max(updated_at) from int_zendesk.zendesk_users_base64"
    cur.execute(sql_query)
    result = cur.fetchone()
    max_updated_timestamp = result[0]
    sql_query = "select EXTRACT(EPOCH FROM TIMESTAMP WITH time zone '"+max_updated_timestamp+"') "
    cur.execute(sql_query)
    result = cur.fetchone()
    max_updated_epoch = int(result[0])
    users_df = pd.DataFrame()
    stream_end = False
    while stream_end == False:
    #users_json = ''  # clears the previous api pull
        result_generator = zenpy_client.users.incremental(start_time=max_updated_epoch)
        df = pd.DataFrame()
        for user in result_generator:
            users_json = user.to_json()
            df = pd.json_normalize(json.loads(users_json))
            users_df = pd.concat([users_df, df],ignore_index=True)
        stream_end = result_generator.end_of_stream
        if stream_end == True:
            break
    df = users_df
    ###df = users_df[["id","name","email","updated_at"]]
except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'fetch data from api', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Users Incremental Load Base64 Failed- Fetch API data process need to be fixed, try to compare data type from API."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit()

else:
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'fetch data from api', current_timestamp,"+ str(len(users_df)) +", 'Success', NULL)")
    conn.commit()
try:
    #df_photo_thumbnails = pd.DataFrame ()
    #df_photo_thumbnails = df['photo.thumbnails'].apply ( pd.Series )
    for col in df.columns:
        if infer_dtype ( df[col] ) == 'mixed':
            # ‘mixed’ is the catchall for anything that is not otherwise specialized
            df[col] = df[col].astype ( 'str' )
    df['tags'] = df['tags'].map ( lambda x: str ( x ).lstrip ( '[' ).rstrip ( ']' ) ).astype ( str )
    df['date_inserted'] = dt.datetime.now ()
    #df['email'] = df['email'].mask ( df['email'].notnull (), np.nan )
except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'data cleanup and manipulation', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Users Incremental Load Base64 Failed- Cleaning data process need to be fixed, try to chaeck data type in database."
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
    df['email'] = df['email'].replace ( np.nan, 'no_email', regex=True )
    df['email'] = df.email.str.encode('utf-8', 'strict').apply(base64.b64encode)
    df['email'] = df['email'].astype ( str )
    df['email'] = df['email'].replace ( "b'", '', regex=True )
    df['email'] = df['email'].replace ( "'", '', regex=True )
except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'encypt pii data', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Users Incremental Load Base64 Failed- Encrypt data process need to be fixed, try to chaeck data type in database."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit()
else:
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'encypt pii data', current_timestamp, NULL, 'Success', NULL)")
    conn.commit()
try:
    df.to_sql('zendesk_users_base64', cnx, schema = 'int_zendesk', method='multi',if_exists='append',index=False)
except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'load data to database', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Users Incremental Load Base64 Failed- Loading data to database need to be fixed, try to chaeck data type in database."
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit()
else:
    cur.execute("update logging.job_summary_logs set status ='Success', end_time = current_timestamp, number_of_records = "+ str(len(df)) +"  where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'load data to database', current_timestamp,"+ str(len(df)) +", 'Success', NULL)")
    conn.commit()

conn.close()
cnx.dispose()
