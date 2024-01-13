import requests
import pandas as pd
import datetime as dt
from datetime import timezone
import json
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from io import StringIO
import numpy as np
from pandas.api.types import infer_dtype
import logging
import logging.handlers
from sqlalchemy import (Column, DateTime, Integer, MetaData,
                        String, Table, create_engine, text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper, sessionmaker
import time
import smtplib

gmail_user = 'bi.dynatrace@gmail.com'
gmail_password = 'biteamisthebest!'
s = smtplib.SMTP('smtp.gmail.com', 587)
s.starttls()
FROM = 'bi.dynatrace@gmail.com'
TO = ["rainy.li@dynatrace.com"]
SUBJECT = "Zendesk Ticket Incremental Load Error (Prod)!"
try:
    host = "ec2-54-161-114-24.compute-1.amazonaws.com"
    dbname = "d5lblgv4vjgptk"
    port = 5432
    user = "ua8hvj8ta2fqn7"
    password = "p587b497a2f1d7bc7b136188eaa840ff3fc5b1180e5c6b47c3413f80915330eb4"
    sslmode = "require"
    postgres_str = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    conn = psycopg2.connect(postgres_str)
    cnx = create_engine(postgres_str)
    conn.autocommit = True
    cur = conn.cursor()
except:
    TEXT = "Hi BI Team, Zendesk Ticket SLA Incremental Load Failed - Database Connection Issue needs to be fixed."
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
    s.login(gmail_user, gmail_password)
    s.sendmail("gmail_user", "rainy.li@dynatrace.com", message)
    s.sendmail("gmail_user", "Bussys_bi@dynatrace.com", message)
    s.sendmail("gmail_user", "rinkesh.pati@dynatrace.com", message)
    s.quit()
else:
    job_name = 'Ticket_SLA_Incremental_Load'

    cur.execute(
        "insert into logging.job_summary_logs(job_name,start_time,end_time,number_of_records,status,message) values ('" + job_name + "', current_timestamp, NULL, NULL,'In Progress',NULL)")
    conn.commit()
    sql_query = "select max(job_execution_id) from logging.job_summary_logs where job_name = '" + job_name + "'"
    cur.execute(sql_query)
    result = cur.fetchone()
    job_exec_id = str(result[0])

    cur.execute(
        "insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'database connection', current_timestamp, NULL, 'Success', NULL)")

    conn.commit()


try:
    sql_query1 = "select max(max_ticket_sync)::timestamp from src_zendesk.ticket_active_slas"
    cur.execute(sql_query1)
    result1 = cur.fetchone()
    max_updated_ticketsla= result1[0]

    sql_query2 = "select EXTRACT(EPOCH FROM TIMESTAMP WITH time zone '" + \
        str(max_updated_ticketsla) + "') "
    cur.execute(sql_query2)
    result2 = cur.fetchone()
    max_epoch_ticket_sla = float(result2[0])

    sql_query3 = 'select max("_fivetran_synced")::timestamp from src_zendesk.ticket'
    cur.execute(sql_query3)
    result3 = cur.fetchone()
    max_updated_ticket = result3[0]

    sql_query4 = "select EXTRACT(EPOCH FROM TIMESTAMP WITH time zone '" + \
        str(max_updated_ticket) + "') "
    cur.execute(sql_query4)
    result4 = cur.fetchone()
    max_epoch_ticket = float(result4[0])

    print('max_updated_ticketmetrics:' + str(max_updated_ticketsla))
    print(max_epoch_ticket_sla)
    print('max_updated_ticket:' + str(max_updated_ticket))
    print(max_epoch_ticket)

    if max_epoch_ticket_sla == max_epoch_ticket:
        print('no more data')
        update_ticket_ids = []
    else:
        sql_query = "select distinct extract(EPOCH from _fivetran_synced) from src_zendesk.ticket where extract(EPOCH from _fivetran_synced) > " + str(
            max_epoch_ticket_sla) + " and extract(EPOCH from _fivetran_synced) <= " + str(max_epoch_ticket)
        cur.execute(sql_query)
        result = [i[0] for i in cur.fetchall()]

        sql_query_tickets = 'select distinct id from src_zendesk.ticket where extract(EPOCH from _fivetran_synced) in  (' + ','.join(
            map(str, result)) + ')'
        cur.execute(sql_query_tickets)
        result_tkt = [i[0] for i in cur.fetchall()]
        update_ticket_ids = result_tkt

except:
    cur.execute(
        "update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute(
        "insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'fetch list of updated ticket ids', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Ticket SLA Incremental Load Failed- could not fetch list of ticket ids."
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
    s.login(gmail_user, gmail_password)
    s.sendmail("gmail_user", "rainy.li@dynatrace.com", message)
    s.sendmail("gmail_user", "Bussys_bi@dynatrace.com", message)
    s.sendmail("gmail_user", "rinkesh.pati@dynatrace.com", message)
    s.quit()
    exit()
else:
    cur.execute(
        "insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'fetch list of updated ticket ids', current_timestamp," + str(
            len(update_ticket_ids)) + ", 'Success', NULL)")
    conn.commit()



try:
    update_ticket_id = update_ticket_ids
    user = '<bussys_data_processing@dynatrace.com>/token'
    pwd = 'toWh50uAmq3KaO8idoDBVaPvV3sOZPCYaA1N6pwd'
    df_data = []
    for update_ticket_id in update_ticket_id:
        url = f'https://dynatrace.zendesk.com/api/v2/tickets/{update_ticket_id}.json?include=slas'
        response = requests.get(url, auth=(user, pwd))
        print(update_ticket_id)
        if response.status_code != 200:
            print(f'Error with status code {response.status_code}')
            continue
        data = response.json()
        ticket_metrics = data.get('ticket', [])
        df_data.append(ticket_metrics)
    def unnest_json(data, prefix='', separator='_'):
        unnested_data = {}
        for key, value in data.items():
            new_key = f"{prefix}{separator}{key}" if prefix else key
            if isinstance(value, dict):
                unnested_data.update(unnest_json(value, new_key, separator))
            else:
                unnested_data[new_key] = value
        return unnested_data
    unnested_data_list = []
    for entry in df_data:
        id_value = entry.get('id')
        slas = entry.get('slas', {}).get('policy_metrics', [])
        for sla in slas:
            unnested_sla = unnest_json(sla, prefix='slas_policy_metrics', separator='_')
            unnested_sla['id'] = id_value
            unnested_data_list.append(unnested_sla)    
            
    df=pd.DataFrame(unnested_data_list)    
    df['date_inserted'] = dt.datetime.now()
    df['type'] = df.index
    df['max_ticket_sync'] = max_updated_ticket
    df['max_ticket_sync'] = pd.to_datetime(df['max_ticket_sync'], utc = True)
except:
    cur.execute(
        "update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute(
        "insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'fetch data from API', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Ticket SLA Incremental Load Failed- API data fetch needs to be fixed."
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
    s.login(gmail_user, gmail_password)
    s.sendmail("gmail_user", "rainy.li@dynatrace.com", message)
    s.sendmail("gmail_user", "Bussys_bi@dynatrace.com", message)
    s.sendmail("gmail_user", "rinkesh.pati@dynatrace.com", message)
    s.quit()
    exit()
else:
    cur.execute(
        "insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'fetch data from API', current_timestamp," + str(len(df_data)) + ", 'Success', NULL)")
    conn.commit()

try:
    df.to_sql('ticket_active_slas', cnx, schema='src_zendesk', method='multi', if_exists='append', index=False, chunksize=5000)

except:
    cur.execute(
        "update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute(
        "insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'load data to database', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Ticket SLA Incremental Load Failed- could not load data to database."
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
    s.login(gmail_user, gmail_password)
    s.sendmail("gmail_user", "rainy.li@dynatrace.com", message)
    s.sendmail("gmail_user", "Bussys_bi@dynatrace.com", message)
    s.sendmail("gmail_user", "rinkesh.pati@dynatrace.com", message)
    s.quit()
    exit()
else:
    cur.execute("update logging.job_summary_logs set status ='Success', end_time = current_timestamp, number_of_records = " +
                str(len(df_data)) + "  where job_execution_id = " + job_exec_id)
    cur.execute(
        "insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'load data to database', current_timestamp," + str(
            len(df_data)) + ", 'Success', NULL)")
    conn.commit()


conn.close()
cnx.dispose()
