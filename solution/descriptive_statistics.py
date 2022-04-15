### Author Suresh Thomas April 2022
from pyspark import SparkContext 
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *

# Create context
sc = SparkContext("local", "uel-unsw-nb15-descr-stat app")

# Set log level to make sure that we dont have unncessary logs
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
    StructField("stcpb",LongType(),True), \
    StructField("dtcpb", LongType(), True), \
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
    StructField("is_sm_ips_ports",IntegerType(),True), \
    StructField("ct_state_ttl",IntegerType(),True), \
    StructField("ct_flw_http_mthd", IntegerType(), True), \
    StructField("is_ftp_login", IntegerType(), True), \
    StructField("ct_ftp_cmd", IntegerType(), True), \
    StructField("ct_srv_src", IntegerType(), True), \
    StructField("ct_srv_dst", IntegerType(), True), \
    StructField("ct_dst_ltm", IntegerType(), True), \
    StructField("ct_src_ ltm", IntegerType(), True), \
    StructField("ct_src_dport_ltm", IntegerType(), True), \
    StructField("ct_dst_sport_ltm", IntegerType(), True), \
    StructField("ct_dst_src_ltm", IntegerType(), True), \
    StructField("attack_cat", StringType(), True), \
    StructField("Label", IntegerType(), True) \

  ])


# Load files

lines = sc.textFile("hdfs/spark/files/UNSW-NB15.csv")
parts = lines.map(lambda l: l.split(","))
attack_record = parts.map(lambda p: (p[0], p[1].strip(),p[2],p[3],p[4],p[5],p[6],p[7],p[8],p[9],p[10],p[11],p[12],p[13],p[14],p[15],p[16],p[17],p[18],p[19],p[20],p[21],p[22],p[23],p[24],p[25],p[26],p[27],p[28],p[29],p[30],p[31],p[32],p[33],p[34],p[35],p[36],p[37],p[38],p[39],p[40],p[41],p[42],p[43],p[44],p[45],p[46],p[47],int(p[48].strip())))

schemaAttackRecords = sqlContext.createDataFrame(attack_record, schema)
schemaAttackRecords.registerTempTable("nb15")
results = sqlContext.sql("SELECT attack_cat,service, Label , count(*) FROM nb15 where Label = 1 GROUP BY  attack_cat, service, Label")

 

#  Write the count to check
print("\n------------------------------------RESULTS BEGIN-----------------------------------\n")
print ("\n Attack category ----------- Service ---------- Total ---------- Label ------------")
results.show()

names = results.map(lambda p: "   " + p.attack_cat + "     " + p.service + "      " + str(p._c3) + "       " + str(p.Label))
for name in names.collect():
  print(name)


print("\n-----------------------------------RESULTS END--------------------------------------------\n")
