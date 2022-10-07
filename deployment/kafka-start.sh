PROJECT_HOME=/tank/hdfs/ruiliu/rotary-aqp

KAFKA_HOME=$PROJECT_HOME/kafka

nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > kafka.out 2>&1 &

