import os
import pandas as pd 
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine, text
from openpyxl import load_workbook

load_dotenv()


#connect to rev ops db
rev_ops_db = f'postgresql://{os.getenv("revops_user")}:{os.getenv("revops_password")}@{os.getenv("revops_host")}:{os.getenv("revops_port")}/{os.getenv("revops_database")}'
rev_ops_db_conn = psycopg2.connect(rev_ops_db)
rev_ops_db_engine = create_engine(rev_ops_db)
rev_ops_db_conn.autocommit = True
rev_ops_db_cur = rev_ops_db_conn.cursor()

def replicate_tables_revops (source_schema, target_schema):
    rev_ops_db_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (source_schema,))
    tables = rev_ops_db_cur.fetchall()

    for table in tables:
        table_name = table[0]

        if table_name.startswith("revops_"):
            print(f"Creating View {target_schema}.{table_name} in revops_analytics..")
            rev_ops_db_cur.execute(f"CREATE OR REPLACE VIEW {target_schema}.revops_analytics_{table_name} as (SELECT * FROM {source_schema}.{table_name});")
        else:
            print(f"Creating Table {target_schema}.{table_name} in revops_analytics..")
            rev_ops_db_cur.execute(f"CREATE TABLE IF NOT EXISTS {target_schema}.{table_name} AS SELECT * FROM {source_schema}.{table_name} WITH DATA;")
    rev_ops_db_conn.commit()
    rev_ops_db_cur.close()
    rev_ops_db_conn.close()

source_schema = 'revops'
target_schema = 'revops_analytics'

replicate_tables_revops(source_schema, target_schema)