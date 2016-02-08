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
    def getReach(partition):
        graph = Graph("http://ec2-52-72-28-165.compute-1.amazonaws.com:7474/db/data/")
        cypher = graph.cypher    
        for triple in partition: # triple = (timestamp, message, id)
            reach = 0
            v = [] # vector of viewers
            tweet = triple[2]
            for viewer in cypher.stream("match (t:Tweet {id:{ID}}) with t match (u)-[r:viewed]->(t) return id(r)", {"ID":tweet}):
                v.append(viewer[0])
            reach = len(set(v))
            impressions = len(v)
            print triple[1], "; reach = ", reach, "impressions = ", impressions

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
            msg = my_tuple[0]
            result.append( (msg, reach, impressions)  )
        return result

    reach = sc.textFile("/home/ubuntu/db/test02/t*", False) \
              .map( lambda x: json.loads(x) ) \
              #.filter( lambda x: x['code']=="tweet" ) \
              .map( lambda x: (x['msg'],  (x['ts'], x['id'])) ) \
              .groupByKey() \
              .mapPartitions(toReach)

    #n_parts = reach._jrdd.splits().size()
    #reach.repartitionAndSortWithinPartitions(n_parts, lambda key: key % n_parts) # load balance

    #reach.foreachPartition(getReach)
    #reach.foreachPartition(toReach)

   
    #reach.collect()
    for q in reach.collect():
     #   pass
        print q

    #reach.foreachPartition(getReach) #mapPartitions(fromPartition)
    
    #// my operator are        schema
    #//                        code id:short            
    #// * add new user     |   1              id:Long ts:Long name:string
    #// * delete user      |   2              id:Long ts:Long name:string
    #// * follow           |   3              id:Long ts:Long follower_id:Long  followed_id:Long
    #// * unfollow         |   4              id:Long ts:Long follower_id:Long  followed_id:Long
    #// * new tweet        |   5              id:Long ts:Long msg:string  user_id:Long
    #// * retweet          |   6              id:Long ts:Long msg:string  user_id:Long original_user_id:Long
 
    
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel( logger.Level.OFF )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.OFF )
    
    sc.stop()


