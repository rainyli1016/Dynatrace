import requests
import pandas as pd
import datetime
from datetime import timezone
import json, sys
import os
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from io import StringIO
import io
import csv
from pandas.api.types import infer_dtype
import smtplib
gmail_user = 'bi.dynatrace@gmail.com'
gmail_password = 'biteamisthebest!'
s = smtplib.SMTP('smtp.gmail.com', 587)
s.starttls()
FROM = 'bi.dynatrace@gmail.com'
TO = ["rainy.li@dynatrace.com"]
SUBJECT = "Zendesk Group Membership Full Load Error (Prod)!"
try:
    host = "ec2-54-161-114-24.compute-1.amazonaws.com"
    dbname = "d5lblgv4vjgptk"
    port = 5432
    user = "int_zendesk_rw"
    password = "p380b89939880318d091085ed2fff9af1d3206575dd74e152eed2027542e86532"
    sslmode = "require"
    postgres_str = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    conn = psycopg2.connect ( postgres_str )
    cnx = create_engine ( postgres_str )
    conn.autocommit = True
    cur = conn.cursor ()
except:
    TEXT = "Hi BI Team, Zendesk Group Memberships Full Load Failed - Database Connection Issue need to be fixed."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
else:
    job_name = 'Group_Membershipd_Full_Load'
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
try:
    url = 'https://dynatrace.zendesk.com/api/v2/group_memberships.json?page[size]=100'
    user = '<bussys_data_processing@dynatrace.com>/token'
    pwd = 'toWh50uAmq3KaO8idoDBVaPvV3sOZPCYaA1N6pwd'
    df_data = pd.DataFrame ()
except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'api connection', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Group Memberships Full Load Failed - API Connection Process need to be fixed."
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
    while url:
        response = requests.get ( url, auth=(user, pwd) )
        data = response.json ()
        group_memberships = data['group_memberships']
        df = pd.DataFrame ( group_memberships )
        for col in df.columns:
            if infer_dtype ( df[col] ) == 'mixed':
                # ‘mixed’ is the catchall for anything that is not otherwise specialized
                df[col] = df[col].astype ( 'str' )
        if data['meta']['has_more']:
            url = data['links']['next']
        else:
            url = None
        df_data = df_data.append ( df )
except:
    cur.execute ("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id )
    cur.execute ("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'fetch data from api', current_timestamp, NULL, 'Failed', NULL)" )
    conn.commit ()
    TEXT = "Hi BI Team, Zendesk Group Memberships Full Load Failed - Fetch Data from Zendesk API process need to be fixed."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit()

else:
    cur.execute ("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'fetch data from api', current_timestamp," + str (
        len ( df ) ) + ", 'Success', NULL)" )
    conn.commit ()
try:
    cur.execute ("truncate table int_zendesk.zendesk_group_memberships")
    df_data.to_sql('zendesk_group_memberships', cnx, schema='int_zendesk', method='multi', if_exists='append', index=False)
except:
    cur.execute ("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id )
    cur.execute ( "insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'load data to database', current_timestamp, NULL, 'Failed', NULL)" )
    conn.commit ()
    TEXT = "Hi BI Team, Zendesk Group Memberships Full Load Failed - Loading data to database process failed and need to be fixed."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
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


