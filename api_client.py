import itertools
from pyspark import SparkContext
sc =SparkContext()

def myParse(line):
    line = line.replace(' -- ', ', ')
    line = line.replace('.rb: ', ', ')
    line = line.replace(', ghtorrent-', ', ')
    return line.split(', ', 4)


def getRDD():
    txtfile=sc.textFile("/suresh/ghtorrent-logs.txt", 4)
    rdd=txtfile.map(myParse)
    return rdd

rowRDD = getRDD().cache()

print(rowRDD.count())

#warnCount = rowRDD.filter(lambda x:x[0]=="WARN")
#print(warnCount.count())

     


#def parseRepos(x):
#    try:
#       split = x[4].split('/')[4:6]
#       joinedSplit = '/'.join(split)
#       result = joinedSplit.split('?')[0]
#    except:
#       result = ''
#    x.append(result)
#    return x

#filteredRDD = rowRDD.filter(lambda x: len(x) == 5)

#apiRDD = filteredRDD.filter(lambda x: x[3] == "api_client")
#reposRDD = apiRDD.map(parseRepos)
#removedEmpty  = reposRDD.filter(lambda x: x[5] != '')
#uniqueRepos = removedEmpty.groupBy(lambda x: x[5])
#print(uniqueRepos.count()) 
