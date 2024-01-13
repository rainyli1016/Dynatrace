import pandas as pd
import datetime as dt
from datetime import timezone
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
host = "ec2-54-161-114-24.compute-1.amazonaws.com"
dbname = "d5lblgv4vjgptk"
port = 5432
user = "datawarehouse_admin"
password = "p89f90adeb858f664ca0c9faaa5b2b3dfd548dfd060e3f3cbd80d7eec121f6e09"
sslmode = "require"
postgres_str = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
conn = psycopg2.connect(postgres_str)
cnx = create_engine(postgres_str)
conn.autocommit = True
cur = conn.cursor()
print('db connection established')
try:
    print('change ownership')
    cur.execute("alter MATERIALIZED VIEW int_varicent.sfdc_bookings_feed_fy24_detail OWNER TO datawarehouse_admin")
    conn.commit()
    cur.execute("alter MATERIALIZED VIEW int_varicent.sfdc_bookings_feed_fy24 OWNER TO datawarehouse_admin")
    conn.commit()
    print('refreshing sfdc_bookings_feed_fy24_detail')
    cur.execute("refresh materialized view int_varicent.sfdc_bookings_feed_fy24_detail")
    conn.commit()
    print('refresh complete - sfdc_bookings_feed_fy24_detail')
# except Exception as e:
#     print('error refreshing int_varicent.sfdc_bookings_feed_fy24_detail')
#     print(e)
    
# try:
    print('refreshing sfdc_bookings_feed_fy24')
    cur.execute("refresh materialized view int_varicent.sfdc_bookings_feed_fy24")
    conn.commit()
    print('refresh complete - sfdc_bookings_feed_fy24')
except Exception as e:
    print('error refreshing materialized views')
    print(e)

try:
    print('executing grants')
    cur.execute("GRANT SELECT ON TABLE int_varicent.sfdc_bookings_feed_fy24 TO boomi_connect")
    cur.execute("GRANT SELECT ON TABLE int_varicent.sfdc_bookings_feed_fy24_detail TO boomi_connect")
    cur.execute("GRANT SELECT ON TABLE int_varicent.sfdc_bookings_feed_fy24_vw TO boomi_connect")
    cur.execute("GRANT SELECT ON TABLE int_varicent.sfdc_bookings_feed_fy24_detail_vw TO boomi_connect")
    conn.commit()
    print('grants complete')
except Exception as e:
    print('error granting permissions')
    print(e)

conn.commit()
