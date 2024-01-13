import urllib3
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
#database
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
#dataframe
import pandas as pd
#api and json
import requests
from requests.exceptions import HTTPError
import json
from json import JSONDecodeError
import functools as ft
import dask.dataframe as dd
import asyncio

def resp_api(s, var_url):
    token = os.getenv('bearer')
    headers = {"accept": "application/json", 
           "Authorization": f"Bearer {token}"}
    base_url = 'https://pra02-api.success.app'
    try:
        response = s.get(f'{base_url}{var_url}',headers=headers)
        response.raise_for_status()
        try:
            j =  response.json()
        except JSONDecodeError:
            print('Response could not be serialized')
        return j
        
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')

def total_page(j):
    last_page = j.get('meta').get('last_page')
    print(f'total page is: {last_page}')
    return last_page

def connect():
        database_url = os.environ.get('database_url_sqla')
        conn = None
        db = create_engine(database_url)
        try:
            print('Connecting to the PostgreSQL...........')
            conn = db.connect()
            print("Connection successfully..................")
        
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            return error
            conn = None

        return conn

def write_json(j,page,table):
    cur_date = datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")
    filename = f'successapp_{table}_{cur_date}_page_{page}.json'
    return filename
    #with open(f'{filename}', 'w') as json_file:
    #    json.dump(j, json_file, indent=4)
    #    return filename

# def projects_flaten(J):
#     # capture bridges information
#     df = pd.DataFrame(j["data"])
#     #if len(sys.argv)>1:
#         #if sys.argv[1]=='csv':
#             #df.to_csv('df.csv')
#     j_bridge = j['data'][0]['bridges']
#     df_bridges = pd.DataFrame.from_dict(j_bridge)
#     df_bridges_id = pd.concat([pd.DataFrame(x) for x in df['bridges']], keys=df['id']).reset_index(level=1, drop=True).reset_index()
#     df_bridges_id.columns = ['id','bridge_service','bridge_type','bridge_resource_id']
#     #if len(sys.argv)>1:
#         #if sys.argv[1]=='csv':
#             #df_bridges_id.to_csv('df_bridges_id.csv')
#     #pivot bridges information
#     df_bridges_id = df_bridges_id.pivot_table ( index=['id', 'bridge_service'], columns='bridge_type', values='bridge_resource_id',aggfunc=lambda x: ' '.join ( x ) )
#     df_bridges_id.reset_index ( inplace=True )
#     #df_bridges_id.to_excel ( 'bridges.xlsx' )
#     # capture tags information
#     df_tag=pd.json_normalize(df['tags'].explode().dropna())
#     df_tag['tags_id']=df_tag['id']
#     df_tag['tags_label']=df_tag['label']
#     df_tag['tags_color']=df_tag['color']
#     df_tag['tags_created_at']=df_tag['created_at']
#     df_tag['tags_updated_at']=df_tag['updated_at']
#     df_tag['tags_association.target_id']=df_tag['association.target_id']
#     df_tag['tags_association.id']=df_tag['association.id']
#     df_tag = df_tag.drop('id', axis=1)
#     df_tag = df_tag.drop('label',axis=1)
#     df_tag = df_tag.drop('color', axis=1)
#     df_tag = df_tag.drop('created_at', axis=1)
#     df_tag = df_tag.drop('updated_at', axis=1)
#     df_tag = df_tag.drop('association.target_id', axis=1)
#     df_tag = df_tag.drop('association.id', axis=1)
#     df_tag['id']=df_tag['association.source_id']
#     df_tag = df_tag.drop('association.source_id', axis=1)
#     #if len(sys.argv)>1:
#         #if sys.argv[1]=='csv':
#             #df_tag.to_csv('df_tag.csv')
#     # capture tags territories
#     df_territories=pd.json_normalize(df['territories'].explode().dropna())
#     df_territories['territories_name']=df_territories['name']
#     df_territories['territories_id']=df_territories['id']
#     df_territories['territories_color']=df_territories['color']
#     df_territories['territories_created_at']=df_territories['created_at']
#     df_territories['territories_updated_at']=df_territories['updated_at']
#     df_territories['territories_association.target_id']=df_territories['association.target_id']
#     df_territories['territories_association.id']=df_territories['association.id']
#     df_territories = df_territories.drop('id', axis=1)
#     df_territories = df_territories.drop('name', axis=1)
#     df_territories = df_territories.drop('color', axis=1)
#     df_territories = df_territories.drop('created_at', axis=1)
#     df_territories = df_territories.drop('updated_at', axis=1)
#     df_territories = df_territories.drop('association.target_id', axis=1)
#     df_territories = df_territories.drop('association.id', axis=1)
#     df_territories['id']=df_territories['association.source_id']
#     df_territories = df_territories.drop('association.source_id', axis=1)
#     #if len(sys.argv)>1:
#        # if sys.argv[1]=='csv':
#             #df_territories.to_csv('df_territories.csv')
#     # capture stage information
#     df_stage_history=pd.json_normalize(df['stage_history'].explode().dropna())
#     df_stage_history['stage_history_id']=df_stage_history['id']
#     df_stage_history['stage_history_created_at']=df_stage_history['created_at']
#     df_stage_history['stage_history_updated_at']=df_stage_history['updated_at']
#     df_stage_history['stage_history_stage_from_id']=df_stage_history['stage_from_id']
#     df_stage_history['stage_history_stage_to_id']=df_stage_history['stage_to_id']
#     df_stage_history['stage_history_status_from']=df_stage_history['status_from']
#     df_stage_history['stage_history_status_to']=df_stage_history['status_to']
#     df_stage_history['stage_history_days']=df_stage_history['days']
#     df_stage_history['stage_history_created_by_type']=df_stage_history['created_by_type']
#     df_stage_history['stage_history_created_by_id']=df_stage_history['created_by_id']
#     df_stage_history['stage_history_updated_by_type']=df_stage_history['updated_by_type']
#     df_stage_history['stage_history_updated_by_id']=df_stage_history['updated_by_id']
#     df_stage_history = df_stage_history.drop('id', axis=1)
#     df_stage_history = df_stage_history.drop('created_at', axis=1)
#     df_stage_history = df_stage_history.drop('updated_at', axis=1)
#     df_stage_history = df_stage_history.drop('stage_from_id', axis=1)
#     df_stage_history = df_stage_history.drop('stage_to_id', axis=1)
#     df_stage_history = df_stage_history.drop('status_from', axis=1)
#     df_stage_history = df_stage_history.drop('status_to', axis=1)
#     df_stage_history = df_stage_history.drop('days', axis=1)
#     df_stage_history = df_stage_history.drop('created_by_type', axis=1)
#     df_stage_history = df_stage_history.drop('created_by_id', axis=1)
#     df_stage_history = df_stage_history.drop('updated_by_type', axis=1)
#     df_stage_history = df_stage_history.drop('updated_by_id', axis=1)
#     df_stage_history['id']=df_stage_history['project_id']
#     df_stage_history = df_stage_history.drop('project_id', axis=1)
#     #if len(sys.argv)>1:
#         #if sys.argv[1]=='csv':
#             #df_stage_history.to_csv('df_stage.csv')
#     # aggregate dataframe
#     df_result = df['result'].apply(pd.Series)
#     df_result['result_id_new'] = df_result['id']
#     df_result['result_id'] = df_result['id']
#     df_result['result_value'] = df_result['value']
#     df_result['result_status'] = df_result['status']
#     df_result['result_created_at'] = df_result['created_at']
#     df_result['result_updated_at'] = df_result['updated_at']
#     df_result = df_result.drop('id', axis=1)
#     df_result = df_result.drop('value', axis=1)
#     df_result = df_result.drop('status', axis=1)
#     df_result = df_result.drop('created_at', axis=1)
#     df_result = df_result.drop('updated_at', axis=1)
#     df_result = df_result.dropna(how='all')
#     df_result = df_result.drop_duplicates()
#     dfs=[df,df_bridges_id,df_tag,df_territories,df_stage_history]
#     df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['id'], how='outer'), dfs)
#     df_final = df_final.drop('bridges',axis=1)
#     df_final = df_final.drop('territories',axis=1)
#     df_final = df_final.drop('tags',axis=1)
#     df_final = df_final.drop('stage_history',axis=1)
#     df_final_r = pd.merge(df_final, df_result, on='result_id', how='left')
#     df_final_r = df_final_r.drop('result', axis=1)
#     df_final_r['id_p'] = df_final_r['id']
#     df_final_r = df_final_r.drop('id', axis=1)
#     df_result_reasons = pd.concat([pd.DataFrame(x) for x in df_final_r['result_reasons']],keys=df_final_r['id_p']).reset_index(level=1, drop=True).reset_index()
#     df_result_reasons['result_reasons_id'] = df_result_reasons['id']
#     df_result_reasons = df_result_reasons.drop('id', axis=1)
#     df_result_reasons['id'] = df_result_reasons['id_p']
#     df_result_reasons = df_result_reasons.drop('id_p', axis=1)
#     df_result_reasons['result_reasons_title'] = df_result_reasons['title']
#     df_result_reasons['result_reasons_created_at'] = df_result_reasons['created_at']
#     df_result_reasons['result_reasons_updated_at'] = df_result_reasons['updated_at']
#     df_result_reasons = df_result_reasons.drop('title', axis=1)
#     df_result_reasons = df_result_reasons.drop('created_at', axis=1)
#     df_result_reasons = df_result_reasons.drop('updated_at', axis=1)
#     df_final_r['id'] = df_final_r['id_p']
#     df_final_r = df_final_r.drop('id_p', axis=1)
#     df_final_r = df_final_r.drop('result_reasons', axis=1)
#     df_final_update = pd.merge(df_final_r, df_result_reasons, on='id', how='left')
#     df_final_update1 = df_final_update.drop_duplicates()
#     #if len(sys.argv)>1:
#         #if sys.argv[1]=='csv':
#             #df_final.to_csv('df_final.csv')
#     return df_final_update1

def convert_df(j):
    df_convert = pd.DataFrame(j["data"])
    return df_convert


if __name__ == '__main__':
    s = requests.Session()
    page = 1
    '''
    # # extract and load project
    # schema = "src_successapp"
    # table = "projects"
    # var_url = f"/v1/dynatrace/public/projects?page={page}&filter[workspace_id]=6afb9636-4c3c-4d3f-b444-b04a4b1e0d55&filter[status]=Queue,Active,Completed&include=tags,territories,bridges,stageHistory,stageHistory.stageFrom,stageHistory.stageTo,result,resultReasons"
    # j = resp_api(s, var_url)
    # last_page = total_page(j)
    # conn = connect()
    # conn.execute(f'TRUNCATE TABLE {schema}.{table}')
    # for page in range(1, last_page+1):
    #     print(f'Current page being processed is: {page} of {last_page}')
    #     var_url = f"/v1/dynatrace/public/projects?page={page}&filter[workspace_id]=6afb9636-4c3c-4d3f-b444-b04a4b1e0d55&filter[status]=Queue,Active,Completed&include=tags,territories,bridges,stageHistory,stageHistory.stageFrom,stageHistory.stageTo,result,resultReasons"
    #     j = resp_api(s, var_url)
    #     filename = write_json(j,page,table)
    #     datafrm_pg = projects_flaten(j)
    #     num_rows = len(datafrm_pg)
    #     start_time = time.time()
    #     datafrm_pg['date_inserted'] = datetime.now ()
    #     datafrm_pg.to_sql(f'{table}', schema=f'{schema}',con=conn, if_exists='append', index=False, chunksize=5000)
    #     duration = format(time.time() - start_time)
    #     now = datetime.now()
    #     print(f'to_sql duration: {duration} seconds.')
    #     #conn.execute(f'INSERT INTO {schema}.logs(table_nm, tot_page, cur_page,var_url,json_nm,to_sql_duration,tot_rows,created_at) VALUES(\'{table}\',{last_page},{page},\'{var_url}\',\'{filename}\',{duration},{num_rows},\'{now}\')')
    '''
    # extract and load stages
    schema = "src_successapp"
    table = "stages"
    var_url = f"/v1/dynatrace/public/stages?page={page}&filter[workspace_id]=6afb9636-4c3c-4d3f-b444-b04a4b1e0d55"
    j = resp_api(s, var_url)
    last_page = total_page(j)
    conn = connect()
    conn.execute(f'TRUNCATE TABLE {schema}.{table}')
    for page in range(1, last_page+1):
        print(f'Current page being processed is: {page} of {last_page}')
        var_url = f"/v1/dynatrace/public/stages?page={page}&filter[workspace_id]=6afb9636-4c3c-4d3f-b444-b04a4b1e0d55"
        j = resp_api(s, var_url)
        filename = write_json(j,page,table)
        datafrm_pg = convert_df(j)
        num_rows = len(datafrm_pg)
        start_time = time.time()
        datafrm_pg.to_sql(f'{table}', schema=f'{schema}',con=conn, if_exists='append', index=False, chunksize=5000)
        duration = format(time.time() - start_time)
        now = datetime.now()
        print(f'to_sql duration: {duration} seconds.')
        #conn.execute(f'INSERT INTO {schema}.logs(table_nm, tot_page, cur_page,var_url,json_nm,to_sql_duration,tot_rows,created_at) VALUES(\'{table}\',{last_page},{page},\'{var_url}\',\'{filename}\',{duration},{num_rows},\'{now}\')')

    # extract and load workspaces
    schema = "src_successapp"
    table = "workspaces"
    var_url = f"/v1/dynatrace/public/workspaces?page={page}"
    j = resp_api ( s, var_url )
    last_page = total_page ( j )
    conn = connect ()
    conn.execute ( f'TRUNCATE TABLE {schema}.{table}' )
    for page in range ( 1, last_page + 1 ):
        print ( f'Current page being processed is: {page} of {last_page}' )
        var_url = f"/v1/dynatrace/public/workspaces?page={page}"
        j = resp_api ( s, var_url )
        filename = write_json ( j, page, table )
        datafrm_pg = convert_df ( j )
        num_rows = len ( datafrm_pg )
        start_time = time.time ()
        datafrm_pg['date_inserted'] = datetime.now ()
        datafrm_pg.to_sql ( f'{table}', schema=f'{schema}', con=conn, if_exists='append', index=False, chunksize=5000 )
        duration = format ( time.time () - start_time )
        now = datetime.now ()
        print ( f'to_sql duration: {duration} seconds.' )
        #conn.execute (f'INSERT INTO {schema}.logs(table_nm, tot_page, cur_page,var_url,json_nm,to_sql_duration,tot_rows,created_at) VALUES(\'{table}\',{last_page},{page},\'{var_url}\',\'{filename}\',{duration},{num_rows},\'{now}\')' )

    # extract and load territories
    schema = "src_successapp"
    table = "territories"
    var_url = f"/v1/dynatrace/public/territories?page={page}"
    j = resp_api ( s, var_url )
    last_page = total_page ( j )
    conn = connect ()
    conn.execute ( f'TRUNCATE TABLE {schema}.{table}' )
    for page in range ( 1, last_page + 1 ):
        print ( f'Current page being processed is: {page} of {last_page}' )
        var_url = f"/v1/dynatrace/public/territories?page={page}"
        j = resp_api ( s, var_url )
        filename = write_json ( j, page, table )
        datafrm_pg = convert_df ( j )
        num_rows = len ( datafrm_pg )
        start_time = time.time ()
        datafrm_pg['date_inserted'] = datetime.now ()
        datafrm_pg.to_sql ( f'{table}', schema=f'{schema}', con=conn, if_exists='append', index=False, chunksize=5000 )
        duration = format ( time.time () - start_time )
        now = datetime.now ()
        print ( f'to_sql duration: {duration} seconds.' )
        #conn.execute (f'INSERT INTO {schema}.logs(table_nm, tot_page, cur_page,var_url,json_nm,to_sql_duration,tot_rows,created_at) VALUES(\'{table}\',{last_page},{page},\'{var_url}\',\'{filename}\',{duration},{num_rows},\'{now}\')' )
   
    # extract and load metas
    schema = "src_successapp"
    table = "metas"
    var_url = f"/v1/dynatrace/public/metas?filter%5Btype%5D=project_result&page={page}"
    j = resp_api(s, var_url)
    last_page = total_page(j)
    conn = connect()
    conn.execute(f'TRUNCATE TABLE {schema}.{table}')
    for page in range(1, last_page + 1):
        print(f'Current page being processed is: {page} of {last_page}')
        var_url = f"/v1/dynatrace/public/metas?filter%5Btype%5D=project_result&page={page}"
        j = resp_api(s, var_url)
        filename = write_json(j, page, table)
        datafrm_pg = convert_df(j)
        num_rows = len(datafrm_pg)
        start_time = time.time()
        datafrm_pg['date_inserted'] = datetime.now()
        datafrm_pg.to_sql(f'{table}', schema=f'{schema}', con=conn,
                          if_exists='append', index=False, chunksize=5000)
        duration = format(time.time() - start_time)
        now = datetime.now()
        print(f'to_sql duration: {duration} seconds.')
        # conn.execute (f'INSERT INTO {schema}.logs(table_nm, tot_page, cur_page,var_url,json_nm,to_sql_duration,tot_rows,created_at) VALUES(\'{table}\',{last_page},{page},\'{var_url}\',\'{filename}\',{duration},{num_rows},\'{now}\')' )


# new project load
http = urllib3.PoolManager()
token = os.getenv('bearer')
headers = {"accept": "application/json", "Authorization": f"Bearer {token}"}
url = 'https://pra02-api.success.app/v1/dynatrace/public/projects?filter[workspace_id]=6afb9636-4c3c-4d3f-b444-b04a4b1e0d55&filter[status]=Queue,Active,Completed&include=tags,territories,bridges,stageHistory,stageHistory.stageFrom,stageHistory.stageTo,result,resultReasons'
response = http.request("GET", url, headers=headers)
if response.status != 200:
    print(response.status)
data_json = response.data.decode('utf8').replace("'", '')
data_json = json.loads(data_json)
last_page = data_json['meta']['last_page']
data_list = []
for page in range(1, last_page+1):
    url = f'https://pra02-api.success.app/v1/dynatrace/public/projects?page={page}&filter[workspace_id]=6afb9636-4c3c-4d3f-b444-b04a4b1e0d55&filter[status]=Queue,Active,Completed&include=tags,territories,bridges,stageHistory,stageHistory.stageFrom,stageHistory.stageTo,result,resultReasons'
    response = http.request("GET", url, headers=headers)
    if response.status != 200:
        print(response.status)
    data_json = response.data.decode('utf8').replace("'", '')
    data_json = json.loads(data_json)
    data_list += data_json['data']
df = pd.DataFrame(data_list)
df_bridges_id = pd.concat([pd.DataFrame(x) for x in df['bridges']],keys=df['id']).reset_index(level=1, drop=True).reset_index()
df_bridges_id.columns = ['id', 'bridge_service','bridge_type', 'bridge_resource_id']
df_bridges_id = df_bridges_id.pivot_table(index=['id', 'bridge_service'], columns='bridge_type', values='bridge_resource_id', aggfunc=lambda x: ' '.join(x))
df_bridges_id.reset_index(inplace=True)
df_tag = pd.json_normalize(df['tags'].explode().dropna())
df_tag['tags_id'] = df_tag['id']
df_tag['tags_label'] = df_tag['label']
df_tag['tags_color'] = df_tag['color']
df_tag['tags_created_at'] = df_tag['created_at']
df_tag['tags_updated_at'] = df_tag['updated_at']
df_tag['tags_association.target_id'] = df_tag['association.target_id']
df_tag['tags_association.id'] = df_tag['association.id']
df_tag = df_tag.drop('id', axis=1)
df_tag = df_tag.drop('label', axis=1)
df_tag = df_tag.drop('color', axis=1)
df_tag = df_tag.drop('created_at', axis=1)
df_tag = df_tag.drop('updated_at', axis=1)
df_tag = df_tag.drop('association.target_id', axis=1)
df_tag = df_tag.drop('association.id', axis=1)
df_tag['id'] = df_tag['association.source_id']
df_tag = df_tag.drop('association.source_id', axis=1)
df_territories = pd.json_normalize(df['territories'].explode().dropna())
df_territories['territories_name'] = df_territories['name']
df_territories['territories_id'] = df_territories['id']
df_territories['territories_color'] = df_territories['color']
df_territories['territories_created_at'] = df_territories['created_at']
df_territories['territories_updated_at'] = df_territories['updated_at']
df_territories['territories_association.target_id'] = df_territories['association.target_id']
df_territories['territories_association.id'] = df_territories['association.id']
df_territories = df_territories.drop('id', axis=1)
df_territories = df_territories.drop('name', axis=1)
df_territories = df_territories.drop('color', axis=1)
df_territories = df_territories.drop('created_at', axis=1)
df_territories = df_territories.drop('updated_at', axis=1)
df_territories = df_territories.drop('association.target_id', axis=1)
df_territories = df_territories.drop('association.id', axis=1)
df_territories['id'] = df_territories['association.source_id']
df_territories = df_territories.drop('association.source_id', axis=1)
df_stage_history = pd.json_normalize(df['stage_history'].explode().dropna())
df_stage_history['stage_history_id'] = df_stage_history['id']
df_stage_history['stage_history_created_at'] = df_stage_history['created_at']
df_stage_history['stage_history_updated_at'] = df_stage_history['updated_at']
df_stage_history['stage_history_stage_from_id'] = df_stage_history['stage_from_id']
df_stage_history['stage_history_stage_to_id'] = df_stage_history['stage_to_id']
df_stage_history['stage_history_status_from'] = df_stage_history['status_from']
df_stage_history['stage_history_status_to'] = df_stage_history['status_to']
df_stage_history['stage_history_days'] = df_stage_history['days']
df_stage_history['stage_history_created_by_type'] = df_stage_history['created_by_type']
df_stage_history['stage_history_created_by_id'] = df_stage_history['created_by_id']
df_stage_history['stage_history_updated_by_type'] = df_stage_history['updated_by_type']
df_stage_history['stage_history_updated_by_id'] = df_stage_history['updated_by_id']
df_stage_history = df_stage_history.drop('id', axis=1)
df_stage_history = df_stage_history.drop('created_at', axis=1)
df_stage_history = df_stage_history.drop('updated_at', axis=1)
df_stage_history = df_stage_history.drop('stage_from_id', axis=1)
df_stage_history = df_stage_history.drop('stage_to_id', axis=1)
df_stage_history = df_stage_history.drop('status_from', axis=1)
df_stage_history = df_stage_history.drop('status_to', axis=1)
df_stage_history = df_stage_history.drop('days', axis=1)
df_stage_history = df_stage_history.drop('created_by_type', axis=1)
df_stage_history = df_stage_history.drop('created_by_id', axis=1)
df_stage_history = df_stage_history.drop('updated_by_type', axis=1)
df_stage_history = df_stage_history.drop('updated_by_id', axis=1)
df_stage_history['id'] = df_stage_history['project_id']
df_stage_history = df_stage_history.drop('project_id', axis=1)
df_result = df['result'].apply(pd.Series)
df_result['result_id_new'] = df_result['id']
df_result['result_id'] = df_result['id']
df_result['result_value'] = df_result['value']
df_result['result_status'] = df_result['status']
df_result['result_created_at'] = df_result['created_at']
df_result['result_updated_at'] = df_result['updated_at']
df_result = df_result.drop('id', axis=1)
df_result = df_result.drop('value', axis=1)
df_result = df_result.drop('status', axis=1)
df_result = df_result.drop('created_at', axis=1)
df_result = df_result.drop('updated_at', axis=1)
df_result = df_result.dropna(how='all')
dfs = [df, df_bridges_id, df_tag, df_territories, df_stage_history]
df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['id'], how='outer'), dfs)
df_final = df_final.drop('bridges', axis=1)
df_final = df_final.drop('territories', axis=1)
df_final = df_final.drop('tags', axis=1)
df_final = df_final.drop('stage_history', axis=1)
df_result= df_result.drop_duplicates()
# df_result = dd.from_pandas(df_result, npartitions=6)
# df_final = dd.from_pandas(df_final, npartitions=6)
df_final_r = pd.merge(df_final, df_result, on='result_id', how= 'left')
#df_final_r = df_final.merge(df_result, how='left', left_index=True, right_index=True, suffixes=('', '_result'))
df_final_r = df_final_r.drop('result',axis=1)
df_final_r['id_p'] = df_final_r['id']
df_final_r = df_final_r.drop('id', axis=1)
#df_final_r = df_final_r.compute()
df_result_reasons = pd.concat([pd.DataFrame(x) for x in df_final_r['result_reasons']],keys=df_final_r['id_p']).reset_index(level=1, drop=True).reset_index()
#df_result_reasons = dd.from_pandas(df_result_reasons, npartitions=6)
df_result_reasons['result_reasons_id'] = df_result_reasons['id']
df_result_reasons = df_result_reasons.drop('id', axis=1)
df_result_reasons['id'] = df_result_reasons['id_p']
df_result_reasons = df_result_reasons.drop('id_p', axis=1)
df_result_reasons['result_reasons_title'] = df_result_reasons['title']
df_result_reasons['result_reasons_created_at'] = df_result_reasons['created_at']
df_result_reasons['result_reasons_updated_at'] = df_result_reasons['updated_at']
df_result_reasons = df_result_reasons.drop('title', axis=1)
df_result_reasons = df_result_reasons.drop('created_at', axis=1)
df_result_reasons = df_result_reasons.drop('updated_at', axis=1)
#df_final_r = dd.from_pandas(df_final_r, npartitions=6)
df_final_r['id'] = df_final_r['id_p']
df_final_r = df_final_r.drop('id_p', axis=1)
df_final_r = df_final_r.drop('result_reasons', axis=1)
df_final_r = df_final_r.drop_duplicates()
df_result_reasons = df_result_reasons.drop_duplicates()
df_final_update1 = pd.merge(df_final_r, df_result_reasons, on='id', how='left')
#df_final_update = df_final_r.merge(df_result_reasons, on='id', how='left')
#df_final_update1 = df_final_update.drop_duplicates()
df_final_update1['date_inserted'] = datetime.now()
#df_final_update1 = df_final_update1.compute()
# # Write to HDF5 using Dask

# def process_batch(batch):
#     batch = batch.drop_duplicates()
#     batch['date_inserted'] = datetime.now()
#     return batch
# async def write_to_hdf(df, batch_size):
#     async with pd.HDFStore('memory_mapped_data.h5', 'w') as store:
#         for i in range(0, len(df), batch_size):
#             data_batch = df.iloc[i:i+batch_size]
#             data_batch = process_batch(data_batch)
#             await store.append('df', data_batch, data_columns=True, format='table')
# async def batch():
#     batch_size = 1000
#     await write_to_hdf(df_final_update, batch_size)

# # Check if an event loop is already running
# if asyncio.get_event_loop().is_running():
#     loop = asyncio.get_event_loop()
# else:
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
# asyncio.run(batch())

# Convert Dask DataFrame back to Pandas DataFrame
#df_final_update1 = df_final_update.compute()
conn = connect()
schema = "src_successapp"
table = "stg_projects"
df_final_update1.to_sql('stg_projects',schema='src_successapp',con=conn, if_exists='replace', index=False, chunksize=1000)
columns_to_insert = ['id', 'title', 'description', 'next_step', 'deal_amount', 'start_date', 'due_date', 'estimated_start_date', 'estimated_end_date', 'status', 'health_color', 'customer_id', 'partner_id', 'stage_id', 'workspace_id', 'result_id', 'comment', 'archived_at', 'template_group_id', 'created_by_type', 'created_by_id', 'created_at', 'next_updated_by_id', 'next_updated_at', 'updated_by_type', 'updated_by_id', 'updated_at', 'description_text', 'description_html', 'next_step_text', 'next_step_html', 'bridge_service', 'linked', 'parent', 'tags_id', 'tags_label', 'tags_color', 'tags_created_at', 'tags_updated_at', '"tags_association.target_id"', '"tags_association.id"', 'territories_name', 'territories_id', 'territories_color', 'territories_created_at', 'territories_updated_at', '"territories_association.target_id"', '"territories_association.id"', 'stage_from', '"stage_to.id"', '"stage_to.name"', '"stage_to.status"', '"stage_to.is_active"', '"stage_to.order"', '"stage_to.workspace_id"', '"stage_to.created_by_type"', '"stage_to.created_by_id"', '"stage_to.created_at"', '"stage_to.updated_by_type"',
                     '"stage_to.updated_by_id"', '"stage_to.updated_at"', '"stage_from.id"', '"stage_from.name"', '"stage_from.status"', '"stage_from.is_active"', '"stage_from.order"', '"stage_from.workspace_id"', '"stage_from.created_by_type"', '"stage_from.created_by_id"', '"stage_from.created_at"', '"stage_from.updated_by_type"', '"stage_from.updated_by_id"', '"stage_from.updated_at"', 'stage_history_id', 'stage_history_created_at', 'stage_history_updated_at', 'stage_history_stage_from_id', 'stage_history_stage_to_id', 'stage_history_status_from', 'stage_history_status_to', 'stage_history_days', 'stage_history_created_by_type', 'stage_history_created_by_id', 'stage_history_updated_by_type', 'stage_history_updated_by_id', 'stage_to', 'result_id_new', 'result_value', 'result_status', 'result_created_at', 'result_updated_at', 'result_reasons_id', 'result_reasons_title', 'result_reasons_created_at', 'result_reasons_updated_at', 'date_inserted', 'customer_name', 'total_tasks', 'total_completed_tasks', 'total_success_criteria', 'total_completed_success_criteria', 'active_days', 'stage_active_days']
source_table = "src_successapp.stg_projects"
destination_table = "src_successapp.projects"
conn.execute("truncate table src_successapp.projects")
query = f"INSERT INTO {destination_table} ({', '.join(columns_to_insert)}) " \
        f"SELECT {', '.join(columns_to_insert)} FROM {source_table}"
        
conn.execute(query)
