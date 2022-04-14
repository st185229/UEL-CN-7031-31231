from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sc = SparkContext("local", "uel-unsw-nb15-descr-stat app")

sqlContext = SQLContext(sc)

#df = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs/spark/files/UNSW-NB15.csv")
#df.show()

#df.printChema()



lines = sc.textFile("hdfs/spark/files/UNSW-NB15.csv")
parts = lines.map(lambda l: l.split(","))
src = lines.map(lambda l: l.split(","))
schemaString = "srcip sport dsport	proto	state	dur	sbytes	dbytes	sttl	dttl	sloss	dloss	service	Sload	Dload	Spkts	Dpkts	swin	dwin	stcpb	dtcpb	smeansz	dmeansz	trans_depth	res_bdy_len	Sjit	Djit	Stime	Ltime	Sintpkt	Dintpkt	tcprtt	synack	ackdat	is_sm_ips_ports	ct_state_ttl	ct_flw_http_mthd	is_ftp_login	ct_ftp_cmd	ct_srv_src	ct_srv_dst	ct_dst_ltm	ct_src_ ltm ct_src_dport_ltm	ct_dst_sport_ltm	ct_dst_src_ltm	attack_cat	Label"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)


# Displays the content of the DataFrame to stdoutpply the schema to the RDD.
schemaSrc = sqlContext.createDataFrame(src, schema)

# Register the DataFrame as a table.
schemaSrc.registerTempTable("src")
# SQL can be run over DataFrames that have been registered as a table.
results = sqlContext.sql("SELECT srcip FROM src")

# The results of SQL queries are RDDs and support all the normal RDD operations.
names = results.map(lambda p: "Name: " + p.srcip)
for name in names.collect():
  print(name)


