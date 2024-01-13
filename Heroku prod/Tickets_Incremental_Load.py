import zenpy
from zenpy import Zenpy
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
    user = "int_zendesk_rw"
    password = "p380b89939880318d091085ed2fff9af1d3206575dd74e152eed2027542e86532"
    sslmode = "require"
    postgres_str = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    conn = psycopg2.connect(postgres_str)
    cnx = create_engine(postgres_str)
    conn.autocommit = True
    cur = conn.cursor()
except:
    TEXT = "Hi BI Team, Zendesk Ticket Incremental Load Failed - Database Connection Issue need to be fixed."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
else:
    job_name = 'Tickets_Incremental_Load'
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
    creds = {
    'email' : '<bussys_data_processing@dynatrace.com>',
    'token' : 'toWh50uAmq3KaO8idoDBVaPvV3sOZPCYaA1N6pwd',
    'subdomain': 'dynatrace'
    }
    zenpy_client = Zenpy(**creds)
    credentials = '<bussys_data_processing@dynatrace.com>/token', 'toWh50uAmq3KaO8idoDBVaPvV3sOZPCYaA1N6pwd'
    session = requests.Session()
    session.auth = credentials
    zendesk = 'https://dynatrace.zendesk.com'
    df_data = pd.DataFrame()
    df_data1 = pd.DataFrame ()
except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'api connection', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Ticket Incremental Load Failed- Zendesk API connection need to be Fixed."
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
    url = 'https://dynatrace.zendesk.com/api/v2/ticket_fields.json?page[size]=100'
    user = '<bussys_data_processing@dynatrace.com>/token'
    pwd = 'toWh50uAmq3KaO8idoDBVaPvV3sOZPCYaA1N6pwd'
    dff_data = pd.DataFrame ()
    while url:
        response = requests.get ( url, auth=(user, pwd) )
        dataf = response.json ()
        ticket_fields = dataf['ticket_fields']
        dff = pd.DataFrame (ticket_fields)
        for col in dff.columns:
            if infer_dtype ( dff[col] ) == 'mixed':
                # ‘mixed’ is the catchall for anything that is not otherwise specialized
                dff[col] = dff[col].astype ( 'str' )
        if dataf['meta']['has_more']:
            url = dataf['links']['next']
        else:
            url = None
        dff_data = dff_data.append(dff)
    cur.execute ( "truncate table int_zendesk.zendesk_tickets_fields" )
    dff_data.to_sql ('zendesk_tickets_fields', cnx, schema='int_zendesk', method='multi', if_exists='append', index=False )
    conn.commit ()
    cur.execute ( "select title from int_zendesk.zendesk_tickets_fields" )
    result = cur.fetchall ()
    def join_tuple_string(result) -> str:
        return ' '.join ( result )
    result1 = map ( join_tuple_string, result )
    ft = list ( result1 )
    #ft = [x.lower () for x in ft]
    cur.execute ("SELECT substring(column_name,8) FROM information_schema.columns WHERE table_schema = 'int_zendesk' AND table_name = 'zendesk_tickets' and column_name like 'fields%'" )
    result2 = cur.fetchall ()
    def join_tuple_string(result2) -> str:
        return ' '.join ( result2 )
    result3 = map ( join_tuple_string, result2 )
    t = list ( result3 )
    #t = [x.lower () for x in t]
    column_title = list ( set ( ft ) - set ( t ) )
    if column_title is not None:
        final = ["fields." + item for item in column_title]
        for item in final:
            cur.execute ('alter table int_zendesk.zendesk_tickets add column "'+item+'"varchar')
            conn.commit ()
    else:
        print ( "no new fields update" )
except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'metadata refresh', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Ticket Incremental Load Failed- Meta data refresh process need to be fixed, try to check Zendesk ticket cross join with ticket field process."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
else:
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'metadata refresh', current_timestamp, NULL, 'Success', NULL)")
    conn.commit()

try:
    sql_query = "select max(updated_at) from int_zendesk.zendesk_tickets"
    cur.execute(sql_query)
    result = cur.fetchone()
    max_updated_timestamp = result[0]
    sql_query = "select EXTRACT(EPOCH FROM TIMESTAMP WITH time zone '"+max_updated_timestamp+"') "
    cur.execute(sql_query)
    result = cur.fetchone()
    max_updated_epoch = int(result[0])

###incremental export data from Zendesk API

    tickets_df = pd.DataFrame()
    stream_end = False
    while stream_end == False:
    #tickets_json = ''  # clears the previous api pull
        result_generator = zenpy_client.tickets.incremental(start_time=max_updated_epoch,include='slas', per_page = 500)
        df = pd.DataFrame()
        for ticket in result_generator:
            tickets_json = ticket.to_json()
            df = pd.json_normalize(json.loads(tickets_json))
            tickets_df = pd.concat([tickets_df, df],ignore_index=True)
        stream_end = result_generator.end_of_stream
    #stream_end = True
        if stream_end == True:
            break
except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'fetch data from api', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Ticket Incremental Load Failed- Fetch data from API process need to be Fixed, try to check the data type from API side."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit()
else:
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'fetch data from api', current_timestamp,"+ str(len(tickets_df)) +", 'Success', NULL)")
    conn.commit()

try:
###split 'fields' column

    df = tickets_df
    df_fields = pd.DataFrame()
    df_fields = df['fields'].apply(pd.Series)

### cross join with 'ticket_fields'table
    for i in range ( len ( df_fields.columns ) ):
        exec ( f'list_of_dict_{i} = df_fields[{i}].to_list()' )
        exec ( f"col_ref_{i} = list_of_dict_{i}[0].get('id')" )
        print ( "col_ref_" + str ( i ) + ": " + str ( eval ( "col_ref_" + str ( i ) ) ) )
        exec ( f'list_colval_{i} = []' )
        for item in eval ( "list_of_dict_" + str ( i ) ):
            # exec(f"list_colval_{i}.append(item.get('value')")
            eval ( "list_colval_" + str ( i ) ).append ( item.get ( 'value' ) )
        sql_query = "select title from int_zendesk.zendesk_tickets_fields where id = '" + str (eval ( "col_ref_" + str ( i ) ) ) + "'"
        cur.execute ( sql_query )
        result = cur.fetchone ()
        exec ( f'col_name_{i} = "fields."+str(result[0])' )
        exec ( f"df_col_{i} = pd.DataFrame(list_colval_{i},columns = [col_name_{i}])" )
        # eval("df_col_"+str(i)) = pd.DataFrame(eval("list_colval_"+str(i)), columns = [str(eval("col_ref_"+str(i)))])
        exec ( f"df = pd.concat([df, df_col_{i}], axis=1, join='inner')" )

###clean up data
    for col in df.columns:
        if infer_dtype ( df[col] ) == 'mixed':
        # ‘mixed’ is the catchall for anything that is not otherwise specialized
            df[col] = df[col].astype ( 'str' )
    df['collaborator_ids'] = df['collaborator_ids'].map(lambda x: str(x).lstrip('[').rstrip(']')).astype(str)
    df['tags'] = df['tags'].map(lambda x: str(x).lstrip('[').rstrip(']')).astype(str)
    df['sharing_agreement_ids'] = df['sharing_agreement_ids'].map(lambda x: str(x).lstrip('[').rstrip(']')).astype(str)
    df['follower_ids'] = df['follower_ids'].map(lambda x: str(x).lstrip('[').rstrip(']')).astype(str)
    df['email_cc_ids'] = df['sharing_agreement_ids'].map(lambda x: str(x).lstrip('[').rstrip(']')).astype(str)
    df['followup_ids'] = df['followup_ids'].map(lambda x: str(x).lstrip('[').rstrip(']')).astype(str)
    df['deleted_ticket_form_id'] = np.nan
    df['date_inserted'] = dt.datetime.now()
    df['description'] = df['description'].mask(df['description'].notnull(),np.nan)
    df['fields.Current Summary'] = df['fields.Current Summary'].mask(df['fields.Current Summary'].notnull(),np.nan)
    df.replace('', np.nan, inplace=True)
    df.replace('X', np.nan, inplace=True)
    if 'via.source.from_.ticket_id' in df.columns:
        df1 = df.loc[df['via.source.from_.ticket_id'].notnull ()]
        df1 = df1[['id', 'via.source.from_.ticket_id']]
        df1['via.source.from_.ticket_id'] = df1['via.source.from_.ticket_id'].astype ( str )
        df1['via.source.from_.ticket_id'] = df1['via.source.from_.ticket_id'].replace ( '\.0$', '', regex=True )
        df1['id'] = df1['id'].astype ( str )
        df1 = df1.groupby ( 'via.source.from_.ticket_id' )['id'].apply ( ','.join )
        df1 = pd.DataFrame ( df1 )
        df1 = df1.reset_index ()
        df1['followup_ids_new'] = df1['id']
        df1['id'] = df1['via.source.from_.ticket_id']
        df1 = df1.drop ( 'via.source.from_.ticket_id', axis=1 )
        df2 = pd.read_sql_query ("select distinct * from int_zendesk.zendesk_tickets where followup_ids is null and status='closed'", con=cnx )
        df_followup_id_update = pd.merge ( df1, df2, on=['id'], how='inner' )
        df_followup_id_update['followup_ids_updated'] = df_followup_id_update[['followup_ids', 'followup_ids_new']].apply (lambda x: ', '.join ( x[x.notnull ()] ), axis=1 )
        df_followup_id_update = df_followup_id_update.drop ( 'followup_ids', axis=1 )
        df_followup_id_update = df_followup_id_update.drop ( 'followup_ids_new', axis=1 )
        df_followup_id_update['followup_ids'] = df_followup_id_update['followup_ids_updated']
        df_followup_id_update = df_followup_id_update.drop ( 'followup_ids_updated', axis=1 )
        df_followup_id_update = df_followup_id_update.drop ( 'row_numb', axis=1 )
        df_followup_id_update = df_followup_id_update.drop ( 'date_inserted', axis=1 )
        df_followup_id_update['date_inserted'] = dt.datetime.now ()
        df_followup_id_update.to_sql ( 'zendesk_tickets', cnx, schema='int_zendesk', method='multi', if_exists='append',index=False )
    else:
        pass

except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'data cleanup and manipulation', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Ticket Incremental Load Failed- Data cleanup and manipulation process need to be fixed."
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
    update_ticket_id = df['id'].tolist ()
    for update_ticket_id in update_ticket_id:
        url = f'{zendesk}/api/v2/tickets/{update_ticket_id}/metrics.json'
        response = session.get ( url )
        if response.status_code != 200:
            print ( f'Error with status code {response.status_code}' )
            cur.execute ("insert into int_zendesk.zendesk_failed_records_ticket_matrics values ('" + job_exec_id + "','" + str (update_ticket_id) + "','" + str ( response.status_code ) + "','" + str (dt.datetime.now () ) + "')" )
            continue
        data = response.json ()
        ticket_metrics = data['ticket_metric']
        df_metrics = pd.DataFrame ( ticket_metrics )
        for col in df_metrics.columns:
            if infer_dtype ( df_metrics[col] ) == 'mixed':
             ##‘mixed’ is the catchall for anything that is not otherwise specialized
                df_metrics[col] = df_metrics[col].astype ( 'str' )
        df_data = df_data.append ( df_metrics )
        df_data['date_inserted'] = dt.datetime.now ()
        df_data['type'] = df_data.index
        
except:
    cur.execute ("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id )
    cur.execute ("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'fetch data for ticket metrics', current_timestamp, NULL, 'Failed', NULL)" )
    conn.commit ()
    TEXT = "Hi BI Team, Zendesk Ticket Incremental Load Failed- fetch ticket metrics process need to be fixed."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit()
else:
    cur.execute ("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'fetch data for ticket metrics', current_timestamp," + str (
        len ( df_data ) ) + ", 'Success', NULL)" )
    conn.commit ()

try:
    update_ticket_id1 = df.loc[df['status'] != 'deleted']['id'].tolist()
    for update_ticket_id1 in update_ticket_id1:
        url = f'{zendesk}/api/v2/tickets/{update_ticket_id1}/side_conversations.json'
        response = session.get ( url )
        if response.status_code != 200:
            print ( f'Error with status code {response.status_code}' )
            cur.execute("insert into int_zendesk.zendesk_failed_records_side_conv values ('" + job_exec_id + "','"+str(update_ticket_id1)+"','"+str(response.status_code)+"','"+str(dt.datetime.now ())+"')")
            continue
        data = response.json ()
        side_conversations = data['side_conversations']
        df_side_conversations = pd.DataFrame ( side_conversations )
        for col in df_side_conversations.columns:
            if infer_dtype ( df_side_conversations[col] ) == 'mixed':
            # ‘mixed’ is the catchall for anything that is not otherwise specialized
                df_side_conversations[col] = df_side_conversations[col].astype ( 'str' )
        df_data1 = df_data1.append ( df_side_conversations )
        df_data1['date_inserted'] = dt.datetime.now ()

except:
    cur.execute ("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id )
    cur.execute ("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'fetch data for side conversations', current_timestamp, NULL, 'Failed', NULL)" )
    conn.commit ()
    TEXT = "Hi BI Team, Zendesk Ticket Incremental Load Failed- fetch ticket side conversations process need to be fixed."
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit()
else:
    cur.execute ("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'fetch data for side conversations', current_timestamp," + str (
        len ( df_data1 ) ) + ", 'Success', NULL)" )
    conn.commit ()

###load into data warehouse
try:
   df.to_sql('zendesk_tickets', cnx, schema = 'int_zendesk', method='multi',if_exists='append',index=False)
   df_data.to_sql ( 'zendesk_ticket_metrics', cnx, schema='int_zendesk', method='multi', if_exists='append', index=False )
   df_data1.to_sql ( 'zendesk_side_conversations', cnx, schema='int_zendesk', method='multi', if_exists='append',index=False )
   #cur.execute ( "update int_zendesk.zendesk_side_conversations set ticket_id = replace(ticket_id ,'.0','')" )
   #cur.execute ( "update int_zendesk.zendesk_tickets set assignee_id = replace(assignee_id ,'.0','')" )
   #cur.execute ( "update int_zendesk.zendesk_tickets set organization_id = replace(organization_id ,'.0','')" )
   #cur.execute ( "update int_zendesk.zendesk_tickets set group_id = replace(group_id ,'.0','')" )
except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'load data to database', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Ticket Incremental Load Failed- Loading data to database process need to be fixed, check both ticket and ticket metrics objects"
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit()
else:
    #cur.execute("update logging.job_summary_logs set status ='Success', end_time = current_timestamp, number_of_records = "+ str(len(df)) +"  where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'load data to database', current_timestamp,"+ str(len(df)) +", 'Success', NULL)")
    conn.commit()

### clean duplicates for follow_up_ids
try:
    cur.execute ("call int_zendesk.zendesk_followup_ids_dupelicates_remove()")
except:
    cur.execute (
        "update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id )
    cur.execute (
        "insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'followup_ids duplicates for parend clean up', current_timestamp, NULL, 'Failed', NULL)" )
    conn.commit ()
    TEXT = "Hi BI Team, Zendesk Ticket Incremental Load Success But MV Refresh Failed"
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit ()
else:
    #cur.execute ("update logging.job_summary_logs set status ='Success', end_time = current_timestamp, number_of_records = " + str (len ( df ) ) + "  where job_execution_id = " + job_exec_id )
    cur.execute ("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values (" + job_exec_id + ", 'followup_ids duplicates for parend clean up', current_timestamp," + str (len ( df ) ) + ", 'Success', NULL)" )
    conn.commit ()

##refresh MV
try:
  cur.execute ( "refresh materialized view int_zendesk.zendesk_tickets_current_mvw" )
  conn.commit()
  cur.execute ( "refresh materialized view int_zendesk.zendesk_tickets_history_mvw" )
  conn.commit()
  cur.execute ( "refresh materialized view ext_zendesk.zendesk_tickets_ci360_vw" )
  conn.commit()
  cur.execute ( "refresh materialized view ext_support.zendesk_tickets" )
  conn.commit()
  cur.execute ( "refresh materialized view ext_totango.zendesk_tickets" )
  
  
except:
    cur.execute("update logging.job_summary_logs set status ='Failed', end_time = current_timestamp where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'materialized view refresh', current_timestamp, NULL, 'Failed', NULL)")
    conn.commit()
    TEXT = "Hi BI Team, Zendesk Ticket Incremental Load Success But MV Refresh Failed- Materilized View refresh failed"
    message = 'Subject: {}\n\n{}'.format ( SUBJECT, TEXT )
    s.login ( gmail_user, gmail_password )
    s.sendmail ( "gmail_user", "rainy.li@dynatrace.com", message )
    s.sendmail ( "gmail_user", "Bussys_bi@dynatrace.com", message )
    s.sendmail ( "gmail_user", "rinkesh.pati@dynatrace.com", message )
    s.quit ()
    exit()
else:
    cur.execute("update logging.job_summary_logs set status ='Success', end_time = current_timestamp, number_of_records = "+ str(len(df)) +"  where job_execution_id = " + job_exec_id)
    cur.execute("insert into logging.job_details_logs(job_execution_id,step_name,exec_timestamp, number_of_records, status, message) values ("+job_exec_id+", 'materialized view refresh', current_timestamp,"+ str(len(df)) +", 'Success', NULL)")
    conn.commit()

conn.close()
cnx.dispose()

