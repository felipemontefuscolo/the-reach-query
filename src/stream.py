#from __future__ import print_function # print rdd
import sys
import time
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row # needed to transform PipelinedRDD to RDD for the functions sortByKey
#import pyspark
from py2neo import Graph, Node, authenticate, Relationship

# u = user id
# ts = timestamp
# follows = rdd containing json with the pair (follower, followed)
def getFollowers(u, ts, follows, unfollows):
    "get followers from a given user"
    return 0

def filterNewUser(x):    xj = json.loads(x); return True if ('code' in xj and xj['code'] == 1) else False
def filterDeleteUser(x): xj = json.loads(x); return True if ('code' in xj and xj['code'] == 2) else False
def filterFollow(x):     xj = json.loads(x); return True if ('code' in xj and xj['code'] == 3) else False
def filterUnFollow(x):   xj = json.loads(x); return True if ('code' in xj and xj['code'] == 4) else False
def filterNewTweet(x):   xj = json.loads(x); return True if ('code' in xj and xj['code'] == 5) else False
def filterReTweet(x):    xj = json.loads(x); return True if ('code' in xj and xj['code'] == 6) else False


if __name__ == "__main__":

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    #sc = SparkContext("local[2]", "Stream")
    sc = SparkContext() # loads config from ./bin/spark-submit
    ssc = StreamingContext(sc, 1)
    sql = SQLContext(sc)

    graph =Graph("http://ec2-52-72-28-165.compute-1.amazonaws.com:7474/db/data/")
    cypher = graph.cypher

    graph.delete_all()
    cypher.execute("CREATE CONSTRAINT ON (n:User) ASSERT n.id IS UNIQUE")


    #all_ops = sc.textFile("./db/*/",False)
    all_ops = sc.textFile("./db/*/",False) \
             .map(lambda x : (json.loads(x)['ts'], x)) \
             .sortByKey()
    #         .map(lambda x : (x[0], pushOp(x[1]))
             
    #for val in all_ops.collect(): print val


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
    rddQueue = []
    rddQueue.append(all_ops)
    
    # Create the QueueInputDStream and use it do some processing
    inputStream = ssc.queueStream(rddQueue)
    #mappedStream = inputStream.map(lambda x : (json.loads(x)['ts'], x)) \
     #                         .sortByKey()


    def pushNode(x):
       tx.append("MERGE (n:User { id:{N} }) RETURN n", {"N": x['id']})
       tx.process()

    def popNode(x):
       tx.append("MATCH (n:User { id:{N} }) DELETE n", {"N": x['id']})
       tx.process()

    def pushFollow(x):
       tx.append("MATCH (b:User {id:{AA}}) WITH b MATCH (a:User {id:{BB}}) CREATE (a)-[:follows]->(b)", { "AA": x['follower_id'], "BB": x['followed_id'] })
       tx.process()

    def popFollow(x):
       tx.append("MATCH (b:User {id:{AA}}) WITH b MATCH (a:User {id:{BB}}) DELETE (a)-[:follows]->(b)", { "AA": x['follower_id'], "BB": x['followed_id'] })
       tx.process()

    def pushTweet(x):
       tx.append("""MERGE (n:Tweet { id:{TID} })
                    ON CREATE SET n.msg=TMSG
                    WITH n
                    MATCH (u:User { id:{UID}})
                    CREATE (u)-[:tweeted]->(n)
                    """, {"TID": x['id'], "UID": x['user_id']})
       tx.process()

    def pushOp(x):
        y = json.loads(x)
        return {
            1 : pushNode(y),
            2 : popNode(y),
            3 : pushFollow(y),
            4 : popFollow(y),
            5 : pushTweet(y)
        }[x]

    
    tx = graph.cypher.begin()

    def echo(time, rdd):
        counts = "Counts at time %s %s" % (time, rdd.collect())
        print(counts)
    
    tx.commit() 

    #inputStream.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
    inputStream.foreachRDD(echo)

    #inputStream.pprint()
   
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel( logger.Level.OFF )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.OFF )
    
    #ssc.start()             # Start the computation
    #ssc.awaitTermination()  # Wait for the computation to terminate
   
    #df = sqlContext.read.json("./db") 

 
    ssc.start()
    time.sleep(6)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
    #sc.stop()


