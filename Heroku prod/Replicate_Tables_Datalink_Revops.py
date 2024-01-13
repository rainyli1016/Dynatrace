import os
import pandas as pd 
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine, text
from openpyxl import load_workbook

load_dotenv()

#connect to bi prod db
bi_prod_db = f'postgresql://{os.getenv("postgres_user")}:{os.getenv("postgres_password")}@{os.getenv("postgres_host")}:{os.getenv("postgres_port")}/{os.getenv("postgres_database")}'
bi_prod_db_conn = psycopg2.connect(bi_prod_db)
bi_prod_db_engine = create_engine(bi_prod_db)
bi_prod_db_conn.autocommit = True
bi_prod_db_cur = bi_prod_db_conn.cursor()



#connect to rev ops db
rev_ops_db = f'postgresql://{os.getenv("revops_user")}:{os.getenv("revops_password")}@{os.getenv("revops_host")}:{os.getenv("revops_port")}/{os.getenv("revops_database")}'
rev_ops_db_conn = psycopg2.connect(rev_ops_db)
rev_ops_db_engine = create_engine(rev_ops_db)
rev_ops_db_conn.autocommit = True
rev_ops_db_cur = rev_ops_db_conn.cursor()

def replicate_tables_revops(schema_list, table_list):
    for schema, table in zip(schema_list, table_list):
        # Check object type from excel list
        result = bi_prod_db_cur.execute("SELECT c.relname AS object_name, \
                                        n.nspname AS schema_name, \
                                        CASE \
                                            WHEN c.relkind = 'r' THEN 'Table' \
                                            WHEN c.relkind = 'v' THEN 'View' \
                                            WHEN c.relkind = 'm' THEN 'Materialized View' \
                                            ELSE 'Unknown' \
                                        END AS object_type \
                                    FROM pg_class c \
                                    JOIN pg_namespace n ON n.oid = c.relnamespace \
                                    WHERE c.relname = '"+table+"' \
                                    AND n.nspname = '"+schema+"'")
        bi_prod_db_conn.commit()
        row = bi_prod_db_cur.fetchall()
        if row is not None:
            if 'Table' in row[0]:
                # Extract meta data from Table
                print(f"Extracting Table meta data from {schema}.{table}...")
                extract_meta_data_source = bi_prod_db_cur.execute("SELECT '(' || pg_catalog.array_to_string(pg_catalog.array_agg(pg_catalog.quote_ident(a.attname) || ' ' || pg_catalog.quote_ident(t.typname)), ', ') \
                                                                FROM pg_class c \
                                                                JOIN pg_attribute a ON a.attrelid = c.oid \
                                                                JOIN pg_type t ON a.atttypid = t.oid \
                                                                JOIN pg_namespace n ON n.oid = c.relnamespace \
                                                                WHERE c.relname = '"+table+"' \
                                                                AND n.nspname = '"+schema+"' \
                                                                AND a.attnum > 0 \
                                                                AND c.relkind IN ('r');")
                bi_prod_db_conn.commit()
                extract_meta_data_source = bi_prod_db_cur.fetchall()
                extract_meta_data_source_str = str(extract_meta_data_source[0])[3:-3] 
                print(f"Table Meta data successfully extracted from {schema}.{table}!")  
                #create foreign tables in target(revops)
                print(f"Creating foreign table {table} in datalink_revops...")
                rev_ops_db_cur.execute(f"create foreign table if not exists datalink_revops.{table} ({extract_meta_data_source_str}) server postgresql_polished_48400 options (schema_name '{schema}', table_name '{table}');")
                rev_ops_db_conn.commit()
                print(f"Successfully created {table} in datalink_revops!")

                #insert table information in logging.rep_tables using existing format
                print(f"Inserting table {table}'s information into logging.rep_tables...")
                rev_ops_db_cur.execute("insert into logging.rep_tables (table_nm,src_schema_nm,tgt_schema_nm,active,insert_date ) \
                                                values('"+table+"','datalink_revops','revops',true,now());")
                rev_ops_db_conn.commit()
                print(f"Successfully inserted table {table}'s information into logging.rep_tables!")
            
            elif 'Materialized View' in row[0]:
                # Extract meta data from materialized view
                print(f"Extracting Materialized View meta data from {schema}.{table}...")
                extract_meta_data_source = bi_prod_db_cur.execute("SELECT '(' || pg_catalog.array_to_string(pg_catalog.array_agg(pg_catalog.quote_ident(a.attname) || ' ' || pg_catalog.quote_ident(t.typname)), ', ') \
                                                                FROM pg_class c \
                                                                JOIN pg_attribute a ON a.attrelid = c.oid \
                                                                JOIN pg_type t ON a.atttypid = t.oid \
                                                                JOIN pg_namespace n ON n.oid = c.relnamespace \
                                                                WHERE c.relname = '"+table+"' \
                                                                AND n.nspname = '"+schema+"' \
                                                                AND a.attnum > 0 \
                                                                AND c.relkind IN ('m');")
                bi_prod_db_conn.commit()
                extract_meta_data_source = bi_prod_db_cur.fetchall()
                extract_meta_data_source_str = str(extract_meta_data_source[0])[3:-3] 
                print(f"Materialized View Meta data successfully extracted from {schema}.{table}!")  
                #create foreign tables in target(revops)
                print(f"Creating foreign table {table} in datalink_revops...")
                rev_ops_db_cur.execute(f"create foreign table if not exists datalink_revops.{table} ({extract_meta_data_source_str}) server postgresql_polished_48400 options (schema_name '{schema}', table_name '{table}');")
                rev_ops_db_conn.commit()
                print(f"Successfully created {table} in datalink_revops!")

                #insert table information in logging.rep_tables using existing format
                print(f"Inserting table {table}'s information into logging.rep_tables...")
                rev_ops_db_cur.execute("insert into logging.rep_tables (table_nm,src_schema_nm,tgt_schema_nm,active,insert_date ) \
                                                values('"+table+"','datalink_revops','revops',true,now());")
                rev_ops_db_conn.commit()
                print(f"Successfully inserted table {table}'s information into logging.rep_tables!")
            
            else:
                # Extract meta data from view
                print(f"Extracting View meta data from {schema}.{table}...")
                extract_meta_data_source = bi_prod_db_cur.execute("SELECT '(' || pg_catalog.array_to_string(pg_catalog.array_agg(pg_catalog.quote_ident(a.attname) || ' ' || pg_catalog.quote_ident(t.typname)), ', ') \
                                                                FROM pg_class c \
                                                                JOIN pg_attribute a ON a.attrelid = c.oid \
                                                                JOIN pg_type t ON a.atttypid = t.oid \
                                                                JOIN pg_namespace n ON n.oid = c.relnamespace \
                                                                WHERE c.relname = '"+table+"' \
                                                                AND n.nspname = '"+schema+"' \
                                                                AND a.attnum > 0 \
                                                                AND c.relkind IN ('v');")
                bi_prod_db_conn.commit()
                extract_meta_data_source = bi_prod_db_cur.fetchall()
                extract_meta_data_source_str = str(extract_meta_data_source[0])[3:-3]
                print(f"View Meta data successfully extracted from {schema}.{table}!")  
                #create foreign tables in target(revops)
                print(f"Creating foreign table {table} in datalink_revops...")
                rev_ops_db_cur.execute(f"create foreign table if not exists datalink_revops.{table} ({extract_meta_data_source_str}) server postgresql_polished_48400 options (schema_name '{schema}', table_name '{table}');")
                rev_ops_db_conn.commit()
                print(f"Successfully created {table} in datalink_revops!")

                #insert table information in logging.rep_tables using existing format
                print(f"Inserting table {table}'s information into logging.rep_tables...")
                rev_ops_db_cur.execute("insert into logging.rep_tables (table_nm,src_schema_nm,tgt_schema_nm,active,insert_date ) \
                                                values('"+table+"','datalink_revops','revops',true,now());")
                rev_ops_db_conn.commit()
                print(f"Successfully inserted table {table}'s information into logging.rep_tables!")
    print("Replication to revops completed!")

df = pd.read_excel(r'/mnt/c/Users/eric.ma/OneDrive - Dynatrace/Desktop/dt_revops_replication.xlsx', sheet_name = 'table access')
schema_list = df['table_schema'].tolist()
table_list = df['table_name'].tolist()

#replicate_tables_revops(schema_list, table_list)

def resolve_datatype_mismatch ():
    source_schema = "datalink_revops"
    target_schema = "revops"
    fields = ["oppty_payment_term", "standard_payment_term", "oppty_payment_term_status", "account_payment_term", "account_payment_term_status"]

    rev_ops_db_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (source_schema,))
    tables = rev_ops_db_cur.fetchall()

    for table in tables:
        table_name = table[0]

        rev_ops_db_cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s", (source_schema, table_name))
        columns = rev_ops_db_cur.fetchall()
        
        all_present = all(field in [col[0] for col in columns] for field in fields)
        
        if all_present:
            print(f"Creating View {target_schema}.{table_name} in revops..")
            rev_ops_db_cur.execute(f"CREATE OR REPLACE VIEW {target_schema}.revops_{table_name} as (SELECT * FROM {source_schema}.{table_name});")
    
    rev_ops_db_conn.commit()
    rev_ops_db_cur.close()
    rev_ops_db_conn.close()

resolve_datatype_mismatch()

# def drop_all_tables(schema):

#     # Set the search path to the target schema
#     rev_ops_db_cur.execute(f"SET search_path TO {schema}")

#     # Retrieve the list of table names in the schema
#     rev_ops_db_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (schema, ))
#     table_names = rev_ops_db_cur.fetchall()

#     # Drop each table in the schema
#     for table_name in table_names:
#         rev_ops_db_cur.execute(f"DROP FOREIGN TABLE IF EXISTS {schema}.{table_name[0]}")

#     # Commit the changes and close the connection
#     rev_ops_db_conn.commit()
#     rev_ops_db_cur.close()
#     rev_ops_db_conn.close()

# # Usage
# drop_all_tables("datalink_revops")