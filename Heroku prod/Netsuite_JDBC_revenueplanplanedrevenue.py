import os 
from dotenv import load_dotenv
import pyspark
import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from pyspark.sql.functions import add_months, last_day, date_trunc, to_date, current_date, col, lit, current_timestamp, date_format, date_add


load_dotenv()
# Add both netsuite and postgres drivers
script_dir = os.path.dirname(os.path.abspath(__file__))
nqjc_driver_path = os.path.join(script_dir, "lib", "NQjc.jar")
postgres_driver_path = os.path.join(script_dir, "lib", "postgresql-42.6.0.jar")
spark = SparkSession.builder \
    .appName("NetsuiteJDBC") \
    .config("spark.jars", ','.join([nqjc_driver_path, postgres_driver_path])) \
    .config("spark.sql.session.timeZone", "America/New_York") \
    .getOrCreate()
url = os.getenv('netsuite_jdbc_url')
revenueplanplannedrevenue_dbtable = os.getenv(
    'netsuite_jdbc_revenueplanplannedrevenue_dbtable')
accountingperiod_dbtable = os.getenv('netsuite_jdbc_accountingperiod_dbtable')
user = os.getenv('netsuite_jdbc_user')
password = os.getenv('netsuite_jdbc_password')
driver = os.getenv('netsuite_jdbc_driver')
# First table revenueplanplannedrevenue
rppr = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", revenueplanplannedrevenue_dbtable) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", driver) \
    .load()
# Second table accountingperiod
ap= spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", accountingperiod_dbtable) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", driver) \
    .load()
#get netsuite system date
system_date = spark.read.format("jdbc") \
    .option("url", url) \
    .option("driver", driver) \
    .option("dbtable", "(select SYSDATE AS sysdate from DUAL)") \
    .option("user", user) \
    .option("password", password) \
    .load()
sysdate = system_date.select("SYSDATE").collect()[0][0].strftime('%Y-%m-%d')
print(sysdate)

end_date1 = date_format(date_trunc("day", last_day(add_months(lit(sysdate), -1))), "yyyy-MM-dd").alias("sysdate")
end_date1_value = spark.range(1).select(end_date1.alias("end_date")).collect()[0]["end_date"]
print(end_date1_value)

end_date2 = date_format(date_trunc("day", last_day(add_months(lit(sysdate), -2))), "yyyy-MM-dd").alias("sysdate")
end_date2_value = spark.range(1).select(end_date2.alias("end_date")).collect()[0]["end_date"]
print(end_date2_value)

enddate_value = date_format(date_add(ap.enddate, 1),"yyyy-MM-dd").alias("enddate")
enddate_values = ap.select(enddate_value).distinct().collect()
for row in enddate_values:
    print(row[0])


result = rppr.join(ap, (rppr.postingperiod == ap.id)) \
    .filter(rppr.postingperiod.isNotNull()) \
    .filter(ap.isposting == 'T') \
    .filter(((date_trunc("day", last_day(add_months(lit(sysdate), -2))) == date_add(ap.enddate, 1).cast("date")) | (date_trunc("day", last_day(add_months(lit(sysdate), -1))) == date_add(ap.enddate, 1).cast("date")))).select(rppr["postingperiod"]).distinct() \
    .orderBy(rppr["postingperiod"].desc())

postingperiod_list = [str(row.postingperiod) for row in result.collect()]
print(postingperiod_list)
result.show()
current_time_str = current_timestamp().cast("string").alias("current_time")
current_time_value = spark.range(1).select(
    current_time_str).collect()[0]["current_time"]

# Print the current timestamp
print(current_time_value)
if postingperiod_list and len(postingperiod_list) > 1:
    filtered_df = rppr.filter((rppr.postingperiod == postingperiod_list[0]) | (
        rppr.postingperiod == postingperiod_list[1]))
elif postingperiod_list:
    filtered_df = rppr.filter(rppr.postingperiod == postingperiod_list[0])
else:
    filtered_df = rppr

filtered_df = filtered_df.withColumn("date_inserted", lit(current_time_value).cast("timestamp"))

db_url = os.getenv('netsuite_jdbc_db_url')
table = "raw_netsuite.revenueplanplannedrevenue_pm"
properties = {
    "user": os.getenv('postgres_user'),
    "password": os.getenv('postgres_password'),
    "driver": "org.postgresql.Driver",
    "batchsize": "10000"
}
spark.read.jdbc(url=db_url, table=table, properties=properties) \
     .limit(0) \
     .write \
     .option("truncate", True) \
     .jdbc(url=db_url, table=table, mode="overwrite", properties=properties)
filtered_df.write.jdbc(url=db_url, table=table, mode="append", properties=properties)
print("Load Completed")
