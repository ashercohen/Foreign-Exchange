11676 Big Data Analytics - Assignment #4 - Cassandra Integration
================================================================

Asher Cohen asherc@cs.cmu.edu

Files:
------
- CassandraDT.java - Implementation of a decision tree over cassandra
- CassandraRandomForest.java - Implementation of a decision tree over cassandra

CassandraDT class extends hw2's DecisionTree class and overrides methods responsible for loading datasets and storing results/serialized models
CassandraRandomForest class extends hw3's RandomForest class and also used CassandraDT to implement the model over Cassandra data store.

I have used the same train/test datasets from hw2:
- hw2_test_train.txt - contains 23245 examples / 24 features
- hw2_test_data.txt - contains 5811 examples / 24 features

