PROJECT_HOME=/tank/hdfs/ruiliu/rotary-aqp

KAFKA_HOME=$PROJECT_HOME/kafka

HADOOP_HOME=$PROJECT_HOME/hadoop

nohup $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties > zookeeper.out 2>&1 &
sleep 1
nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > kafka.out 2>&1 &
sleep 1
$PROJECT_HOME/scripts/create-topic.sh
sleep 1
$HADOOP_HOME/bin/hdfs namenode -format
