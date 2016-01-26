// usage within spark-shell: HdfsWordCount.main(Array("hdfs://quickstart.cloudera:8020/user/cloudera/sparkStreaming/"))

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Impressions {
 def main(args: Array[String]) {

   val public_dns : String =sys.env("PUBLIC_DNS")
   //val dns = "hdfs://ec2-52-70-131-91.compute-1.amazonaws.com:9000"
   val dns = "hdfs://ec2-52-70-131-91.compute-1.amazonaws.com:9000"
   println(dns)

   // setup the Spark Context named sc
   val conf = new SparkConf().setAppName("PriceDataExercise")
   val sc = new SparkContext(conf)

   //val file = sc.textFile(dns + "/user/test.txt")

   //val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

   //counts.saveAsTextFile(dns + "/user/out")

   

 }
}


