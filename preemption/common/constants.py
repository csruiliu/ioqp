
class RuntimeConstants:
    # max memory for each executor
    MAX_MEMORY = '10G'

    # entry point for your application
    ENTRY_CLASS = 'ruiliu.aqp.tpch.QueryTPCH'

    # master node url for the cluster
    MASTER = 'spark://southport:7077'

    # supply configuration for jre
    JAVA_OPT = 'spark.executor.extraJavaOptions=-Xms10G -XX:+UseParallelGC -XX:+UseParallelOldGC'

    # entry point for your application
    ENTRY_JAR = '$SPARK_HOME/jars/ruiliu-aqp_2.11-2.4.0.jar'

    # kafka bootstrap server
    BOOTSTRAP_SERVER = 'lincoln:9092'

    # number of batch for input data
    BATCH_NUM = 20

    # number of shuffle for partition
    SHUFFLE_NUM = 20

    # query statistical information
    STAT_DIR = '/home/run_scripts/stat_dir'

    # tpch query static files
    TPCH_STATIC_DIR = '/home/run_scripts/tpch_static'

    # scale factor of the input tpch dataset
    SCALE_FACTOR = 5

    # hdfs url for the cluster
    HDFS_ROOT = 'hdfs://southport:9000'

    # 0: querypath-aware
    # 1: subplan-aware
    # 2: IncObv
    # 3: IncStat, collect cardinality groudtruth
    # 4: Run $batch_num, collect selectivities
    EXECUTION_MDOE = 0

    # number of partition for input dataset
    INPUT_PARTITION = 20

    # Performance goal
    # smaller than 1.0 -> latency constraint
    # larger  than 1.0 -> resource constraint
    CONSTRAINT = 0.05

    # largedataset: Q2, Q11, Q13, Q16, Q22
    # smalldataset: Q1, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q12, Q14, Q15, Q17, Q18, Q19, Q20, Q21
    # small, non-incrementable: Q15 Q17 Q18 Q20 Q21
    # executed query: q2, q11, q13, q15, q17, q22
    LARGEDATASET = 'false'

    # 0: turn off iOLAP
    # 1: turn on iOLAP
    IOLAP = 0

    # percentage of choosing the right incrementability
    INC_PERCENTAGE = 1.0

    # bias of statistical information
    COST_BIAS = 1.0

    # max step for query
    MAX_STEP = 100

    # sample time
    SAMPLE_TIME = 0.07

    # sample ratio for running
    SAMPLE_RATIO = 1.0

    # trigger interval (milliseconds)
    TRIGGER_INTERVAL = 100

    # aggregation interval (milliseconds)
    AGGREGATION_INTERVAL = 50

    # checkpoint path
    CHECKPOINT_PATH = 'file:///home/tpch-checkpoint'
