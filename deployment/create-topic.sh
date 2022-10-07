#!/bin/sh

PROJECT_HOME=/tank/hdfs/ruiliu/rotary-aqp

KAFKA_HOME=$PROJECT_HOME/kafka

PARTITION=1
RF=1

$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --create --topic Supplier --partitions $PARTITION --replication-factor $RF
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --create --topic Part --partitions $PARTITION --replication-factor $RF
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --create --topic PartSupp --partitions $PARTITION --replication-factor $RF
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --create --topic Customer --partitions $PARTITION --replication-factor $RF
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --create --topic Orders --partitions $PARTITION --replication-factor $RF
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --create --topic Lineitem --partitions $PARTITION --replication-factor $RF
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --create --topic Nation --partitions $PARTITION --replication-factor $RF
$KAFKA_HOME/bin/kafka-topics.sh -zookeeper localhost:2181 --create --topic Region --partitions $PARTITION --replication-factor $RF
