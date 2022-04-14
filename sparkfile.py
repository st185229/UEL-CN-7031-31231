from pyspark import SparkContext
from pyspark import SparkFiles
finddistance = "hdfs:///suresh/ghtorrent-logs.txt"
finddistancename = "ghtorrent-logs.txt"
sc = SparkContext("local", "SparkFile App")
sc.addFile(finddistance)
print "Absolute Path -> %s" % SparkFiles.get(finddistancename)
