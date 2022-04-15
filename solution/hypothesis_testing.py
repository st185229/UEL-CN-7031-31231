### Author Suresh Thomas April 2022
from pyspark import SparkContext 
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import HiveContext

# Create context
sc = SparkContext("local", "uel-unsw-nb15-descr-stat app")

# Set log level to make sure that we dont have unncessary logs
sc.setLogLevel("ERROR")

# SQL context
sqlContext = HiveContext(sc)


# Define Schema
schema = StructType([ 
    StructField("srcip",StringType(),True), StructField("sport",IntegerType(),True), \
    StructField("dstip",StringType(),True),  \
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
attack_record = parts.map(lambda p: (p[0], int(p[1].strip()),p[2],int(p[3]),p[4],p[5], \
                float(p[6]),int(p[7]),int(p[8]),int(p[9]),int(p[10]), int(p[11]), \
                int(p[12]),p[13],float(p[14]),float(p[15]),int(p[16]), int(p[17]), \
                int(p[18]),int(p[19]),int(p[20]),int(p[21]),int(p[22]),int(p[23]), \
                int(p[24]),int(p[25]),float(p[26]),float(p[27]),int(p[28]),int(p[29]), \
                float(p[30]),float(p[31]),float(p[32]),float(p[33]), float(p[34]),int(p[35]), \
                int(p[36]),int(p[37]),int(p[38]),int(p[39]),int(p[40]),int(p[41]),int(p[42]), int(p[43]),int(p[44]),int(p[45]),int(p[46]),p[47],int(p[48].strip())))

schemaAttackRecords = sqlContext.createDataFrame(attack_record, schema)
schemaAttackRecords.registerTempTable("nb15")


#  Write the count to check
print("\n------------------------------------RESULTS BEGIN-----------------------------------\n")




print("\n-----------------------------------RESULTS END--------------------------------------------\n")

