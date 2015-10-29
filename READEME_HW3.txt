11676 Big Data Analytics - Assignment #3 - Random Forest
=========================================================

Asher Cohen asherc@cs.cmu.edu

Files:
------
- RandomForest.java - random forest implementation using hw2's decision tree
- DecisionTree.java - hw2's implementation of decision tree

I have used the same train/test datasets from hw2:
- hw2_test_train.txt - contains 23245 examples / 24 features
- hw2_test_data.txt - contains 5811 examples / 24 features


Summary Statistics:
-------------------

I have built a forest with 10 trees, here are the statistics:

accuracy on train set - 80% of the entire dataset (random 2/3 used for build a tree, 1/3 to test on forest)

number of trees | tree accuracy | forest accuracy

    1               0.505614        0.505614
    2               0.492709        0.547038
    3               0.507549        0.562653
    4               0.517099        0.602529
    5               0.495806        0.594270
    6               0.517486        0.625242
    7               0.496967        0.672474
    8               0.495161        0.669377
    9               0.503807        0.705381
    10              0.513357        0.705639
