#!/bin/sh

PROJECT_HOME=/tank/hdfs/ruiliu/rotary-aqp

SPARK_HOME=$PROJECT_HOME/spark
DATA_ROOT=$PROJECT_HOME/tpch-data/SF1

checkpoint=hdfs://roscoe:9000/tpch_checkpoint

master=local[20]
#master=local
largedataset=false

$SPARK_HOME/bin/spark-submit \
    --class ruiliu.aqp.tpch.LoadTPCH \
    --master $master $SPARK_HOME/jars/ruiliu-aqp_2.11-2.4.0.jar \
    roscoe:9092 \
    $DATA_ROOT \
    $checkpoint \
    $largedataset

