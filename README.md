# BigDataComputing-UniPd

This repository contains the homework assignments from the "Big Data Computing" course as part of the Master's degree in Data Science at the University of Padova. These assignments were completed as a teamwork collaboration.

The main focus of these homework assignments was to explore and utilize parallelism using Apache Spark.

## Homework 1
Implement and test in Spark two **MapReduce** algorithms to **count the number of distinct triangles in an undirected graph**.
- Algorithm 1 (*MR_ApproxTCwithNodeColors*) returns an **approximation** of the number of triangles, and uses a hash function for dividing the edges **deterministic**ally into **partitions**.
- Algorithm 2 (*MR_ApproxTCwithSparkPartitions*) returns the precise number of triangles, and utilizes the **partitions provided by Spark**.

[1_triangleCounting_MR.py](https://github.com/Di40/BigDataComputing-UniPd/blob/main/1_triangleCounting_MR.py)  

## Homework 2
Run a Spark program on the **CloudVeneto cluster**. As for HW 1, the objective is to estimate (approximately or exactly) the number of triangles in an undirected graph, using two new algorithms.

[2_MapReduce_CloudVenetoCluster.py](https://github.com/Di40/BigDataComputing-UniPd/blob/main/2_MapReduce_CloudVenetoCluster.py)  

## Homework 3
Use the **Spark Streaming API** to devise a program which **processes a stream of items** and assesses experimentally the space-accuracy tradeoffs featured by the **count sketch** to estimate the individual frequencies of the items and the second moment F2.

[3_streaming_countSketch.py](https://github.com/Di40/BigDataComputing-UniPd/blob/main/3_streaming_countSketch.py)  
 
# Team
| | |
| :---: | :---: |
| [Dejan Dichoski](https://github.com/Di40) | [Marija Cveevska](https://github.com/marijacveevska) |


*Feel free to explore, learn, and adapt these assignments for your own projects. Happy coding and data crunching!*
