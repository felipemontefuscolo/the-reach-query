#from __future__ import print_function # print rdd
import sys
import time
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext # needed to transform PipelinedRDD to RDD for the functions sortByKey
import pyspark

# u = user id
# ts = timestamp
# follows = rdd containing json with the pair (follower, followed)
#def getFollowers(u, ts, follows, unfollows)
#    "get followers from a given user"
#    return 0

def filterNewUser(x):    xj = json.loads(x); return True if ('code' in xj and xj['code'] == 1) else False
def filterDeleteUser(x): xj = json.loads(x); return True if ('code' in xj and xj['code'] == 2) else False
def filterFollow(x):     xj = json.loads(x); return True if ('code' in xj and xj['code'] == 3) else False
def filterUnFollow(x):   xj = json.loads(x); return True if ('code' in xj and xj['code'] == 4) else False
def filterNewTweet(x):   xj = json.loads(x); return True if ('code' in xj and xj['code'] == 5) else False
def filterReTweet(x):    xj = json.loads(x); return True if ('code' in xj and xj['code'] == 6) else False

if __name__ == "__main__":

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext("local[2]", "NetworkWordCount")
    ssc = StreamingContext(sc, 1)
    sql = SQLContext(sc)
   
    lines = sc.textFile("./db", 1)
    follows = lines.filter(filterReTweet)
    #lines.filter()
    #counts = lines.map(lambda x: x)
    #              .map(lambda x: (x, 1)) \
    #             .reduceByKey(lambda a, b: a + b) \
    #             .sortByKey(True) 
    # Try this after .reduceByKey: .map(lambda x:(x[0],x[1])) \ # which is faster? I don't know   
    for val in follows.collect(): print val
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


