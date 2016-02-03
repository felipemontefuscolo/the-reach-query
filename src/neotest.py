import sys
import time
import json

from py2neo import Graph, Node, authenticate, Relationship



#authenticate("http://ec2-52-72-28-165.compute-1.amazonaws.com:7474", "neo4j", "bssefur3")
graph =Graph("http://ec2-52-72-28-165.compute-1.amazonaws.com:7474/db/data/")
cypher = graph.cypher

#alice = Node("User", name="Alice")
#bob = Node("User", name="Bob")
#alice_knows_bob = Relationship(alice, "KNOWS", bob)
#graph.create(alice_knows_bob)

#result = graph.cypher.execute('MATCH (a)-[]->(b) RETURN id(a), id(b)')
#for row in result:
#    print(row[0], row[1])

for r in graph.cypher.stream("MATCH (a)-[]->(b) RETURN a.id, b.id"):
    print (r[0],r[1])

for v in graph.cypher.stream("match (n:User {id:101}) with n match (p-->n) return p.id"):
    print v[0]

print graph.cypher.execute("match (n:User {id:101}) with n match (p-->n) return p.id")

