PROJECT_HOME=/tank/hdfs/ruiliu/rotary-aqp

KAFKA_HOME=$PROJECT_HOME/kafka

$KAFKA_HOME/bin/kafka-server-stop.sh > zookeeper.out 2>&1 &

