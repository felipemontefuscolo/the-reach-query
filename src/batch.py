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
   
    def getViewers(xx): # x = (timestamp, message)
        pass

    # The py2neo connection here is not serializable as it opens connections
    # to the target DB that are bound to the machine where it's created.
    # The solution is create a cypher.graph instance per partition
    # Ref: http://stackoverflow.com/a/28024183/1842038
    def getReach(partition):
        graph = Graph("http://ec2-52-72-28-165.compute-1.amazonaws.com:7474/db/data/")
        cypher = graph.cypher    
        for pair in partition: # pair (tweet, users)
            reach = 0
            f = []
            f.extend(pair[1])
            for u in pair[1]:
                for v in cypher.stream("match (n:User {id:{ID}}) with n match (p-->n) return p.id", {"ID":u}):
                    f.append(v[0])
            reach = len(set(f))
            impressions = len(f)
            print pair[0], "; reach = ", reach, "impressions = ", impressions


    reach = sc.textFile("/home/ubuntu/db/simple_test/*", False) \
              .map( lambda x: json.loads(x) ) \
              .filter( lambda x: x['code']=="tweet" ) \
              .map( lambda x: (x['ts'], x['msg'], x['id']))      # (timestamp, message)

    n_parts = reach._jrdd.splits().size()

    reach.repartitionAndSortWithinPartitions(n_parts, lambda key: key % n_parts)
     #    .reduceByKey(lambda a,b: concat(a,b))

    for q in reach.collect():
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


