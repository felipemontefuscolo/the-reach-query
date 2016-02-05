from __future__ import print_function # print rdd
import sys
import time
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row # needed to transform PipelinedRDD to RDD for the functions sortByKey
#import pyspark
from py2neo import Graph, Node, authenticate, Relationship
from pyspark.streaming.kafka import KafkaUtils


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage:submit ....  stream.py <zk> <topic>")
        exit(-1)

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    #sc = SparkContext("local[2]", "Stream")
    sc = SparkContext() # loads config from ./bin/spark-submit
    ssc = StreamingContext(sc, 1)
    sql = SQLContext(sc)

    graph =Graph("http://ec2-52-72-28-165.compute-1.amazonaws.com:7474/db/data/")
    cypher = graph.cypher

    graph.delete_all()
    cypher.execute("CREATE CONSTRAINT ON (n:User)  ASSERT n.id IS UNIQUE")
    cypher.execute("CREATE CONSTRAINT ON (n:Tweet) ASSERT n.id IS UNIQUE")

    zkQuorum, topic = sys.argv[1:] 
    #test = sc.textFile("/home/ubuntu/the-reach-query/src/db/*",False)
    all_ops = KafkaUtils.createStream(ssc, zkQuorum, "Graph1", {topic: 1}) 
    all_ops.pprint()
    all_ops.map(lambda x : (json.loads(x)['ts'], x))
    #all_ops = sc.textFile("/home/ubuntu/db/test01/x*",False) \
    #         .map(lambda x : (json.loads(x)['ts'], x)) \
    #         .sortByKey()
             
    #for val in all_ops.collect(): print val

    #// my operator are        schema
    #//                        code id:short            
    #// * add new user     |   1              id:Long ts:Long name:string
    #// * delete user      |   2              id:Long ts:Long name:string
    #// * follow           |   3              id:Long ts:Long follower_id:Long  followed_id:Long
    #// * unfollow         |   4              id:Long ts:Long follower_id:Long  followed_id:Long
    #// * new tweet        |   5              id:Long ts:Long msg:string  user_id:Long
 
    # Create the queue through which RDDs can be pushed to
    # a QueueInputDStream
    #rddQueue = []
    #rddQueue.append(all_ops)
    
    # Create the QueueInputDStream and use it do some processing
    #inputStream = ssc.queueStream(rddQueue)
    #mappedStream = inputStream.map(lambda x : (json.loads(x)['ts'], x)) \
     #                         .sortByKey()


    def pushNode(x):
       return cypher.execute("MERGE (n:User { id:{N} }) ON CREATE SET n.name={NAME} RETURN n", {"N": x['id'], "NAME": x['name']})

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
       return cypher.execute("""MERGE (t:Tweet { id:{TID} })
                                ON CREATE SET t.msg={TMSG}
                                WITH t
                                MATCH (u:User { id:{UID}})
                                CREATE (u)-[r:tweeted {ts:{TS}}]->(t)
                                CREATE (u)-[l:viewd {ts:{TS}}]->(t)
                                WITH t,u
                                MATCH (f)-->(u)
                                CREATE (f)-[:viewd {ts:{TS}}]->(t)""", {"TID":x['id'], "UID":x['user_id'], "TMSG":x['msg'], "TS":x['ts'] })

    def pushOp(y):
        x = json.loads(y)
        funcs = {
            "user" :     pushNode,
            "del_user" : popNode,
            "follow" :   pushFollow,
            "unfollow" : popFollow,
            "tweet" :    pushTweet
        }
        ret = funcs[x['code']](x)
    
    #tx = graph.cypher.begin()

    def processRDD(timei, rdd):
        c = rdd.collect()
        #print(c)
        for cc in c:
            pushOp(cc[1])
        #rdd.foreach(pp)
    
    #tx.commit() 

    #inputStream.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
    #inputStream.foreachRDD(processRDD)
    all_ops.foreachRDD(processRDD)

    #inputStream.pprint()
   
    #logger = sc._jvm.org.apache.log4j
    #logger.LogManager.getLogger("org").setLevel( logger.Level.OFF )
    #ogger.LogManager.getLogger("akka").setLevel( logger.Level.OFF )
    
    #ssc.start()             # Start the computation
    #ssc.awaitTermination()  # Wait for the computation to terminate
   
    #df = sqlContext.read.json("./db") 

 
    ssc.start()
    #time.sleep(20)
    #ssc.stop(stopSparkContext=True, stopGraceFully=True)
    ssc.awaitTermination()
    #sc.stop()


