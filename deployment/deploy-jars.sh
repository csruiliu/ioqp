
PROJECT_HOME=/tank/hdfs/ruiliu/rotary-aqp

cp $PROJECT_HOME/kafka/libs/kafka-clients-2.1.0.jar $PROJECT_HOME/spark/jars/
cp $PROJECT_HOME/rotary-aqp/aqp/target/scala-2.11/ruiliu-aqp_2.11-2.4.0.jar $PROJECT_HOME/spark/jars/
cp $PROJECT_HOME/rotary-aqp/sql/catalyst/target/scala-2.11/spark-catalyst_2.11-2.4.0.jar $PROJECT_HOME/spark/jars/
cp $PROJECT_HOME/rotary-aqp/external/avro/target/scala-2.11/spark-avro_2.11-2.4.0.jar $PROJECT_HOME/spark/jars/
cp $PROJECT_HOME/rotary-aqp/external/kafka-0-10-sql/target/scala-2.11/spark-sql-kafka-0-10_2.11-2.4.0.jar $PROJECT_HOME/spark/jars/
