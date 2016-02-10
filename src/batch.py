#from __future__ import print_function # print rdd
import sys
import time
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext # needed to transform PipelinedRDD to RDD for the functions sortByKey
import pyspark
from py2neo import Graph, Node, authenticate, Relationship


if __name__ == "__main__":

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext() # loads config from ./bin/spark-submit
    sql = SQLContext(sc)
   

    # The py2neo connection here is not serializable as it opens connections
    # to the target DB that are bound to the machine where it's created.
    # The solution is create a cypher.graph instance per partition
    # Ref: http://stackoverflow.com/a/28024183/1842038
    def toReach(partition):
        graph = Graph("http://ec2-52-72-28-165.compute-1.amazonaws.com:7474/db/data/")
        cypher = graph.cypher
        result = []
        for my_tuple in partition:
            viewers = []
            for i in my_tuple[1]:
                tweet_id = i[1]
                for viewer in cypher.stream("match (t:Tweet {id:{ID}}) with t match (u)-[:viewed]->(t) return id(u)", {"ID":tweet_id}):
                    viewers.append(viewer[0])
            reach = len(set(viewers))
            impressions = len(viewers)
            print my_tuple[0], "; reach = ", reach, " impressions = ", impressions, " viewers = ", viewers
            msg = '"' + my_tuple[0] + '"'
            result.append( (msg,   (reach, impressions)  )  )
        return result

    def toCSVLine(data):
        return ','.join(str(d) for d in data) + ',0'
    def toCSVLine1(data):
        return ','.join(str(d) for d in data) + ',1'
    def toCSVLine2(data):
        return ','.join(str(d) for d in data) + ',2'

    reach = sc.textFile("hdfs://ec2-52-70-131-91.compute-1.amazonaws.com:9000/db/test01/t*") \
              .map( lambda x: json.loads(x) ) \
              .map( lambda x: (x['msg'],  (x['ts'], x['id'])) ).cache()

    mean = (reach.top(1)[0][1][0] + reach.take(1)[0][1][0]) // 2
    #print(mean, reach.take(1)[0][1][0])

    reach1 = reach.filter(lambda t: t[1][0] <= mean) 
    reach2 = reach.filter(lambda t: t[1][0] > mean) 

    reach  =reach.groupByKey() \
                 .mapPartitions(toReach) \
                 #.sortByKey() \
                 #.map(lambda z: (z[0], z[1][0], z[1][1])) \
                 #.map(toCSVLine)

    reach1  =reach1.groupByKey() \
                 .mapPartitions(toReach) \
                 #.map(toCSVLine1)

    reach2  =reach2.groupByKey() \
                 .mapPartitions(toReach) \
                 #.map(toCSVLine2)

    reach1 = reach1.union(reach2) \
                   .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
                   #.sortByKey() \
                   #.map(lambda z: (z[0], z[1][0], z[1][1])) \
                   #.map(toCSVLine2)

    temp = reach1.union(reach) \
                 .reduceByKey(lambda a,b: (1.*abs(a[0]-b[0])/(a[0]+b[0]), 1.*abs(a[1]-b[1])/(a[1]+b[1])  )  ) \
                 .map(lambda z: (z[1][0], z[1][1])) \
                 .reduce(lambda a,b: (a[0]+b[0], a[1]+b[1]))
    res = [  1.*temp[0] / reach.count(), 1.*temp[1] / reach.count()]

    print "\n\n ERROR = ", str(res),"\n\n"


    #n_parts = reach._jrdd.splits().size()
    #reach.repartitionAndSortWithinPartitions(n_parts, lambda key: key % n_parts) # load balance

    #reach.foreachPartition(getReach)
    #reach.foreachPartition(toReach)

   
    #reach.collect()
    #lines.coalesce(1, True).saveAsTextFile('file:///home/ubuntu/db/reach.csv')
    print("\n\nPRINTING RESULTS\n\n")
    f = open("/home/ubuntu/db/reach.csv", 'w')
    reach = reach.map(lambda z: (z[0], z[1][0], z[1][1])).map(toCSVLine)
    for q in reach.collect():
        f.write(q+"\n")
    f.close()

    f1 = open("/home/ubuntu/db/reach1.csv", 'w')
    reach1 = reach1.map(lambda z: (z[0], z[1][0], z[1][1])).map(toCSVLine1)
    for q in reach1.collect():
        f1.write(q+"\n")
    f1.close()

    #f2 = open("/home/ubuntu/db/reach2.csv", 'w')
    #for q in reach2.collect():
    #    f2.write(q+"\n")
    #f2.close()

    #reach.foreachPartition(getReach) #mapPartitions(fromPartition)
    
    #// my operator are        schema
    #//                        code id:short            
    #// * add new user     |   1              id:Long ts:Long name:string
    #// * delete user      |   2              id:Long ts:Long name:string
    #// * follow           |   3              id:Long ts:Long follower_id:Long  followed_id:Long
    #// * unfollow         |   4              id:Long ts:Long follower_id:Long  followed_id:Long
    #// * new tweet        |   5              id:Long ts:Long msg:string  user_id:Long
    #// * retweet          |   6              id:Long ts:Long msg:string  user_id:Long original_user_id:Long
 
    
    #logger = sc._jvm.org.apache.log4j
    #logger.LogManager.getLogger("org").setLevel( logger.Level.OFF )
    #logger.LogManager.getLogger("akka").setLevel( logger.Level.OFF )
    
    sc.stop()


