spark-submit --name "ReachStreaming" --jars kafka_jar/spark-streaming-kafka-assembly_2.10.jar  ~/the-reach-query/src/stream.py localhost:2181 graph1
