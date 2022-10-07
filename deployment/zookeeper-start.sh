PROJECT_HOME=/tank/hdfs/ruiliu/rotary-aqp

KAFKA_HOME=$PROJECT_HOME/kafka

nohup $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties > zookeeper.out 2>&1 &

