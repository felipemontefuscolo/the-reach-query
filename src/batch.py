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
   
    

    def concat(a,b):
        c = a
        c.extend(b)
        c = list(set(c))
        return c

    def getReach(users):
        "users = vector of users"
        f = []
        f.extend(users)
        #cypher.execute("match (n:User {id:{ID}}) with n match (p-->n) return p.id", {"ID":u})
        #cypher.execute("match (n) return n")
        #for u in users:
        #    for v in graph.cypher.stream("match (n:User {id:{ID}}) with n match (p-->n) return p.id", {"ID":u}):
        #        f.append(v)
        return len(set(f))
            
    def toTuple(x):
        y = json.loads(x)
        return (y['msg'], y['user_id'])
   
    def fromPartition(part):
        graph =Graph("http://ec2-52-72-28-165.compute-1.amazonaws.com:7474/db/data/")
        cypher = graph.cypher    
        for line in part:
            print(line[0], line[1])

    reach = sc.textFile("./db/tweets/*",False) \
              .map( toTuple ) \
              .reduceByKey(lambda a,b: concat(a,b))
    reach.foreachPartition(fromPartition) #mapPartitions(fromPartition)
    
    #for r in reach:
    #    print(r[0], getReach(r[1]))

    def g(x):
        pass
    reach.foreach(g)


    #counts = lines.map(lambda x: x)
    #              .map(lambda x: (x, 1)) \
    #             .reduceByKey(lambda a, b: a + b) \
    #             .sortByKey(True) 
    # Try this after .reduceByKey: .map(lambda x:(x[0],x[1])) \ # which is faster? I don't know   
    #for val in all_ops.collect(): print val
    #counts.foreach(print)

    #// my operator are        schema
    #//                        code id:short            
    #// * add new user     |   1              id:Long ts:Long name:string
    #// * delete user      |   2              id:Long ts:Long name:string
    #// * follow           |   3              id:Long ts:Long follower_id:Long  followed_id:Long
    #// * unfollow         |   4              id:Long ts:Long follower_id:Long  followed_id:Long
    #// * new tweet        |   5              id:Long ts:Long msg:string  user_id:Long
    #// * retweet          |   6              id:Long ts:Long msg:string  user_id:Long original_user_id:Long
 
    # Create the queue through which RDDs can be pushed to
    # a QueueInputDStream
    #rddQueue = []
    #rddQueue.append(counts)
    
    # Create the QueueInputDStream and use it do some processing
    #inputStream = ssc.queueStream(rddQueue)
    #mappedStream = inputStream.flatMap(lambda x: x.split(' ')) \
    #                          .map(lambda x: (x, 1))
    #reducedStream = mappedStream.reduceByKey(lambda a, b: a + b)
    #sortedStream = reducedStream.sortByKey()
    #inputStream.pprint()
    
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel( logger.Level.OFF )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.OFF )
    
    #ssc.start()             # Start the computation
    #ssc.awaitTermination()  # Wait for the computation to terminate
   
    #df = sqlContext.read.json("./db") 

 
    #ssc.start()
    #time.sleep(6)
    #ssc.stop(stopSparkContext=True, stopGraceFully=True)
    sc.stop()


