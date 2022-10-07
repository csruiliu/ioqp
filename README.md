# Rotary-AQP

A resource arbitration system for approximate query processing jobs, build on top of Apache Spark.

## Prerequisite

+ Java 8
+ Apache Kafka 2.11-2.1.0 
+ Apache Hadoop 2.6.5
+ Apache Spark 2.4.0 (pre-built for Apache Hadoop 2.6)

Any other prerequisites for Apache Spark

## Rotary-AQP Deployment

1. Compile Rotary-AQP and generate the jar file.  
2. Deploy the necessary jar files using `deployment/deploy-jars.sh`
3. Start Zookeeper and Kafka using `deployment/kafka-start.sh` and `deployment/zookeeper-start.sh`
4. Create Kafka topics for the workload (e.g., TPC-H) using `deployment/create-topic.sh`
5. Start Hadoop and Spark
6. Run demo using `deployment/load-data-tpch.sh` 

## Source Code 

+ The source codes of Rotary-AQP Python client are located in `rotary` directory. We can use `runtime_rotary.py` to run Rotary-AQP on the predefined workload (e.g., TPC-H), and exploit `runtime_heuristic.py`, `runtime_relaqs.py`, `runtime_roundrobin.py` to run other baselines on the same workload. The `estimator` directory contains the progress estimators for both Rotary-AQP and ReLAQS (the baseline that needs estimation as well) 

+ The source codes of AQP computation engine for Rotary-AQP are located in `aqp` directory, which can run the jobs from the workload in an AQP way (for example, in batch mode). 

+ To support Rotary-AQP, we also modified the original source code of Apache Spark and added addtional source codes to Apache Spark, most of them are located in `sql/core`, such as `sql/core/src/main/scala/org/apache/spark/spl`, `sql/core/src/main/scala/org/apache/spark/spl/execution`, `sql/core/src/main/scala/org/apache/spark/spl/streaming`. Should you want to chase more technical or implementation details, please check them out.


