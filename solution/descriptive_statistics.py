### Author Suresh Thomas April 2022
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

# Create context
sc = SparkContext("local", "uel-unsw-nb15-descr-stat app")
sc.setLogLevel("ERROR")

# SQL context
sqlContext = SQLContext(sc)


# Define Schema
schema = StructType([ \
    StructField("srcip",StringType(),True), \
    StructField("sport",IntegerType(),True), \
    StructField("dstip",StringType(),True), \
    StructField("dsport", IntegerType(), True), \
    StructField("proto", StringType(), True), \
    StructField("state", StringType(), True), \
    StructField("dur",DoubleType(),True), \
    StructField("sbytes",IntegerType(),True), \
    StructField("dbytes",IntegerType(),True), \
    StructField("sttl", IntegerType(), True), \
    StructField("dttl", IntegerType(), True), \
    StructField("sloss", IntegerType(), True), \
    StructField("dloss",IntegerType(),True), \
    StructField("service",StringType(),True), \
    StructField("Sload",DoubleType(),True), \
    StructField("Dload", DoubleType(), True), \
    StructField("Spkts", IntegerType(), True), \
    StructField("Dpkts", IntegerType(), True), \
    StructField("swin",IntegerType(),True), \
    StructField("dwin",IntegerType(),True), \
    StructField("stcpb",IntegerType(),True), \
    StructField("dtcpb", IntegerType(), True), \
    StructField("smeansz", IntegerType(), True), \
    StructField("dmeansz", IntegerType(), True), \
    StructField("trans_depth",IntegerType(),True), \
    StructField("res_bdy_len",IntegerType(),True), \
    StructField("Sjit",DoubleType(),True), \
    StructField("Djit",DoubleType(),True), \
    StructField("Stime", LongType(), True), \
    StructField("Ltime", LongType(), True), \
    StructField("Sintpkt", DoubleType(), True), \
    StructField("Dintpkt",DoubleType(),True), \
    StructField("tcprtt",DoubleType(),True), \
    StructField("synack",DoubleType(),True), \
    StructField("ackdat",DoubleType(),True), \
    StructField("is_sm_ips_ports",BinaryType(),True), \
    StructField("ct_state_ttl",IntegerType(),True), \
    StructField("ct_flw_http_mthd", IntegerType(), True), \
    StructField("is_ftp_login", BinaryType(), True), \
    StructField("ct_ftp_cmd", IntegerType(), True), \
    StructField("ct_srv_src", IntegerType(), True), \
    StructField("ct_srv_dst", IntegerType(), True), \
    StructField("ct_dst_ltm", IntegerType(), True), \
    StructField("ct_src_ ltm", IntegerType(), True), \
    StructField("ct_src_dport_ltm", IntegerType(), True), \
    StructField("ct_dst_sport_ltm", IntegerType(), True), \
    StructField("ct_dst_src_ltm", IntegerType(), True), \
    StructField("attack_cat", StringType(), True), \
    StructField("Label", BinaryType(), True) \

  ])




lines = sc.textFile("hdfs/spark/files/UNSW-NB15.csv")
parts = lines.map(lambda l: l.split(","))
src = lines.map(lambda l: l.split(","))

#schemaString = "srcip sport dsport	proto	state	dur	sbytes	dbytes	sttl	dttl	sloss	dloss	service	Sload	Dload	Spkts	Dpkts	swin	dwin	stcpb	dtcpb	smeansz	dmeansz	trans_depth	res_bdy_len	Sjit	Djit	Stime	Ltime	Sintpkt	Dintpkt	tcprtt	synack	ackdat	is_sm_ips_ports	ct_state_ttl	ct_flw_http_mthd	is_ftp_login	ct_ftp_cmd	ct_srv_src	ct_srv_dst	ct_dst_ltm	ct_src_ ltm ct_src_dport_ltm	ct_dst_sport_ltm	ct_dst_src_ltm	attack_cat	Label"
#fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
#schema = StructType(fields)


# Displays the content of the DataFrame to stdoutpply the schema to the RDD.
rdd = sqlContext.createDataFrame(src, schema)

# Register the DataFrame as a table.
rdd.registerTempTable("src")

# Cache the data frame
cachedDataFrame = rdd.cache()

#  Write the count to check
print("\n------------------------------------RESULTS BEGIN-----------------------------------\n")
print("Total number of rows :- " , cachedDataFrame.count())
cachedDataFrame.describe()
cachedDataFrame.printSchema()
print("\n-----------------------------------RESULTS END--------------------------------------------\n")


# SQL can be run over DataFrames that have been registered as a table.
#results = sqlContext.sql("SELECT srcip FROM src")

# The results of SQL queries are RDDs and support all the normal RDD operations.
#names = results.map(lambda p: "Name: " + p.srcip)
#for name in names.collect():
#  print(name)


