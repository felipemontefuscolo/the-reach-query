


# Computing Reach and Impressions



The goal of this project is to compute the webpage counterpart of *Unique Visitors* and *Visits* for social networks. Those metrics are called, respectively, *Reach* and *Impressions*.

slides:
http://bit.ly/reachquery

According to Twitter [website](http://brnrd.me/reach-audience-and-impressions-on-twitter-and-beyond), their definition are stated as:

 - **Reach**: number of people who may have seen your content 
 - **Impressions**: number of times the reached people may have seen your content

![Example1](https://raw.githubusercontent.com/felipemontefuscolo/the-reach-query/master/images/ex1.png)

Reach is associated with a time frame: For instance, hourly, daily, monthly, etc..

It is important to note that

**Reach(day 1) + Reach(day 2) &ne; Reach(day 1 + day 2)**


For the graph I tested (generated with `utils/data_generator.py`),
the error between these two quantities is ~ 30%.

If you want to compute the Reach exactly in different granularities, you have to compute the Reach from the most coarse-grained to the most fine-grained time frame.

I used implemented two approaches to compute the Reach, and they are illustrated on the pictures below.

### Approach 1

![enter image description here](https://raw.githubusercontent.com/felipemontefuscolo/the-reach-query/master/images/a1.png)

### Approach 2

![enter image description here](https://raw.githubusercontent.com/felipemontefuscolo/the-reach-query/master/images/a2.png)

## Pipeline

![enter image description here](https://raw.githubusercontent.com/felipemontefuscolo/the-reach-query/master/images/pipeline.png)

The data is engineered and Kafka redirect it to Spark and Spark Streaming. Spark Streaming process requests such as add user, delete user, follow and unfollow and send them to Neo4j throught [Py2Neo](http://py2neo.org/2.0/). Spark takes the graph from Neo4j, compute the reach of tweets and write on HDFS.

----



![Donate](https://www.paypalobjects.com/en_US/GB/i/btn/btn_donateCC_LG.gif)

(Just kidding ...<sub>(or not <sub>(just kidding ...<sub>(or not <sub>(just kidding ...<sub>(or not <sub>(just kidding ...)</sub>)</sub>)</sub>)</sub>)</sub>)</sub>)
