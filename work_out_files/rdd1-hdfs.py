from pyspark import SparkContext
sc = SparkContext("local", "SparkFile App")

def myParse(line):
    line = line.replace(' -- ', ', ')
    line = line.replace('.rb: ', ', ')
    line = line.replace(', ghtorrent-', ', ')
    return line.split(', ', 4)

def getRDD():
    txtfile = sc.textFile("hdfs:///suresh/ghtorrent-logs.txt",4)
    rdd=txtfile.map(myParse)
    return rdd

rowRDD = getRDD().cache()
print(rowRDD.count())

#print(rowRDD.take(2))

#warnCount = rowRDD.filter(lambda x:x[0]=="WARN")
#print(warnCount.count())
