/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
//package org.apache.spark.examples.streaming

import scala.sys.process._ // Execute external command

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.anormcypher._ // spark to neo4j
// without auth

// For TESTing
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Queue
//import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
//import org.apache.spark.streaming.scheduler.*;
//import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.log4j.Logger
import org.apache.log4j.Level


/**
 * Counts words in new text files created in the given directory
 * Usage: HdfsWordCount <directory>
 *   <directory> is the directory that Spark Streaming will use to find and read new text files.
 *
 * To run this on your local machine on directory `localdir`, run this example
 *    $ bin/run-example \
 *       org.apache.spark.examples.streaming.HdfsWordCount localdir
 *
 * Then create a text file in `localdir` and the words in the file will get counted.
 */

//private class MyJobListener(ssc: StreamingContext) extends StreamingListener {
//  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) = synchronized {
//    ssc.stop(true)
//  }
//}

object Impressions {
  def main(args: Array[String]) {
    //if (args.length < 1) {
    //  System.err.println("Usage: HdfsWordCount <directory>")
    //  System.exit(1)
    //}
    val file = "src/main/scala/datadir"
    val outfile = "src/main/scala/out.txt" 

    // suppress INFO
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("Impressions")
    // Create the context
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(4)) // when using sc
    //val ssc = new StreamingContext(sparkConf, Seconds(2))

    // TEST streaming
    val test_rdd  = sc.textFile("src/main/scala/datadir/test.txt")
    val words = test_rdd.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    val test_rddQueue: Queue[RDD[(String, Int)]] = Queue()
    test_rddQueue.enqueue( wordCounts)
    val dstream = ssc.queueStream(test_rddQueue)
    dstream.print()

     // Create the FileInputDStream on the directory and use the
     // stream to count words in new files created
    // val lines = ssc.textFileStream(file)
    // val words = lines.flatMap(_.split(" "))
    // val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

    val data1_dns : String = "ec2-52-72-28-165.compute-1.amazonaws.com"
    val neo4j_db : String = "/home/ubuntu/neo4j-community-2.3.2/data/graph.db" 
    val neo4j_db_dir : String = "/home/ubuntu/neo4j-community-2.3.2/data/" 

    // reset database ... the database is in the node data1
    Process("ssh " + data1_dns + " rm -rf " + neo4j_db)!


    // my operator are        schema
    //                        code id    
    // * add new user     |   1        name:string
    // * delete user      |   2        name:string
    // * follow           |   3        follower:string  followed:string
    // * unfollow         |   4        follower:string  followed:string
    // * new tweet        |   5        msg:string  user:string
    // * retweet          |   6        msg:string  user:string original_user:string
   
     


    implicit val connection = Neo4jREST(data1_dns, 7474, neo4j_db_dir)
    val logData = sc.textFile(FILE, 4).cache()
    val count = logData
      .flatMap( _.split(" "))
      .map( w =>
        Cypher("CREATE(:Word {text:{text}})")
          .on( "text" -> w ).execute()
    ).filter( _ ).count()



    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println

