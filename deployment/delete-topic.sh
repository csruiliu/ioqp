#!/bin/sh

PROJECT_HOME=/tank/hdfs/ruiliu/rotary-aqp

KAFKA_HOME=$PROJECT_HOME/kafka

$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --delete --topic Supplier
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --delete --topic Part
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --delete --topic PartSupp
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --delete --topic Customer
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --delete --topic Orders
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --delete --topic Lineitem
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --delete --topic Nation
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --delete --topic Region
