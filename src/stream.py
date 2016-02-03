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
       return cypher.execute("MERGE (n:User { id:{N} }) RETURN n", {"N": x['id']})

    def popNode(x):
       return cypher.execute("MATCH (n:User { id:{N} }) DELETE n", {"N": x['id']})

    def pushFollow(x):
       return cypher.execute("""MATCH (a:User {id:{AA}})
                                WITH a
                                MATCH (b:User {id:{BB}})
                                CREATE (a)-[r:follows]->(b)
                                return r""", { "AA": x['follower_id'], "BB": x['followed_id'] })

    def popFollow(x):
       return cypher.execute("""MATCH (a:User {id:{AA}})-[r]->(b:User {id:{BB}})
                                delete r""", { "AA": x['follower_id'], "BB": x['followed_id'] })

    def pushTweet(x):
       return cypher.execute("""MERGE (n:Tweet { id:{TID} })
                                ON CREATE SET n.msg={TMSG}
                                WITH n
                                MATCH (u:User { id:{UID}})
                                CREATE (u)-[r:tweeted]->(n)
                                return r""", {"TID":x['id'], "UID":x['user_id'], "TMSG":x['msg'] })

    def pushOp(y):
        x = json.loads(y)
        funcs = {
            1 : pushNode,
            2 : popNode,
            3 : pushFollow,
            4 : popFollow,
            5 : pushTweet
        }
        try:
            ret = funcs[x['code']](x)
        except:
            print "ERRRRRRRRRRRRROOOOOOO: ", x
    
    #tx = graph.cypher.begin()

    def processRDD(timei, rdd):
        c = rdd.collect()
        #print(c)
        for cc in c:
            pushOp(cc[1])
        #rdd.foreach(pp)
    
    #tx.commit() 

    #inputStream.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
    inputStream.foreachRDD(processRDD)

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


