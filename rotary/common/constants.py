
class WorkloadConstants:
    WORKLOAD_SIZE = 30

    # the random seed used for generating
    RANDOM_SEED = 42

    # the parameter used to generate arrival time. Expected number of events occurring in a fixed-time interval
    ARRIVAL_LAMBDA = 4

    # period of scheduling time window (second)
    SCH_ROUND_PERIOD = 60

    # period of checking period (second)
    CHECK_PERIOD = 60

    # period of checkpoint offset (second)
    CKPT_PERIOD = 60

    # queries used in relaqs
    WORKLOAD_RELAQS = ["q1", "q3", "q5", "q6", "q11", "q16", "q19"]

    # all queries in tpch
    WORKLOAD_FULL = ["q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8" "q9", "q10", "q11", "q12",
                     "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22"]

    # the queries cost 5-10 GB when scale factor is 1
    WORKLOAD_LIGHT = ["q1", "q2", "q4", "q6", "q10", "q11", "q12", "q13", "q14", "q15", "q16", "q19", "q22"]
    # the deadline list for workload_light, time unit second
    DEADLINE_LIGHT = [360, 420, 480, 540, 600, 660, 720, 780, 840, 900]
    # the percentage of light workload
    LIGHT_RATIO = 0.4

    # the queries cost 15-30 GB when scale factor is 1
    WORKLOAD_MEDIUM = ["q3", "q5", "q8", "q17", "q20"]
    # the deadline list for workload_medium, time unit second
    DEADLINE_MEDIUM = [1080, 1200, 1320, 1440, 1560, 1680, 1800, 1920, 2040, 2160]
    # the percentage of medium workload
    MEDIUM_RATIO = 0.3

    # the queries cost over 30 GB when scale factor is 1
    WORKLOAD_HEAVY = ["q7", "q9", "q18", "q21"]
    # the deadline list for workload_heavy, time unit second
    DEADLINE_HEAVY = [1440, 1620, 1800, 1980, 2160, 2340, 2520, 2700, 2880, 3060]
    # the percentage of heavy workload
    HEAVY_RATIO = 0.3

    # accuracy threshold for the jobs in the workload
    ACCURACY_OBJECTIVE = [0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]


class MemoryConstants:
    # Medum dimensionality, Result is TPCH scale factor independent
    # Q1, Q3, Q4, Q5, Q6, Q7, Q8, Q12, Q13, Q14, Q16, Q19, Q22

    # High dimensionality, Few results, lot of empty cells
    # Q15, Q18

    # High dimensionality, Result % of scale factor
    # Q2, Q9, Q10, Q11, Q17, Q20, Q21

    # TPCH queries JVM memory allocation for executor under TPCH dataset SF=1
    Q1 = 5
    Q2 = 10
    Q3 = 15
    Q4 = 10
    Q5 = 25
    Q6 = 5
    Q7 = 55
    Q8 = 20
    Q9 = 65
    Q10 = 5
    Q11 = 5
    Q12 = 5
    Q13 = 10
    Q14 = 5
    Q15 = 5
    Q16 = 5
    Q17 = 30
    Q18 = 65
    Q19 = 10
    Q20 = 20
    Q21 = 100
    Q22 = 10


class TPCHAGGConstants:
    # tpch-q1 agg schema
    Q1_AGG_COL = ['sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge',
                  'avg_qty', 'avg_price', 'avg_disc', 'count_order']

    # tpch-q2 agg schema
    Q2_AGG_COL = ['min_supplycost']

    # tpch-q3 agg schema
    Q3_AGG_COL = ['revenue']

    # tpch-q4 agg schema
    Q4_AGG_COL = ['order_count']

    # tpch-q5 agg schema
    Q5_AGG_COL = ['revenue']

    # tpch-q6 agg schema
    Q6_AGG_COL = ['revenue']

    # tpch-q7 agg schema
    Q7_AGG_COL = ['revenue']

    # tpch-q8 agg schema
    Q8_AGG_COL = ['mkt_share']

    # tpch-q9 agg schema
    Q9_AGG_COL = ['sum_profit']

    # tpch-q10 agg schema
    Q10_AGG_COL = ['revenue']

    # tpch-q11 agg schema
    Q11_AGG_COL = ['value']

    # tpch-q12 agg schema
    Q12_AGG_COL = ['low_line_count', 'high_line_count']

    # tpch-q13 agg schema
    Q13_AGG_COL = ['c_count', 'custdist']

    # tpch-q14 agg schema
    Q14_AGG_COL = ['sum_disc_price', 'uadf_q14']

    # tpch-q15 agg schema
    Q15_AGG_COL = ['total_revenue']

    # tpch-q16 agg schema
    Q16_AGG_COL = ['supplier_cnt']

    # tpch-q17 agg schema
    Q17_AGG_COL = ['avg_quantity', 'avg_yearly']

    # tpch-q18 agg schema
    Q18_AGG_COL = ['avg_quantity', 'avg_yearly']

    # tpch-q19 agg schema
    Q19_AGG_COL = ['revenue']

    # tpch-q20 agg schema
    Q20_AGG_COL = ['agg_l_sum']

    # tpch-q21 agg schema
    Q21_AGG_COL = ['numwait']

    # tpch-q22 agg schema
    Q22_AGG_COL = ['avg_acctbal', 'numcust', 'totalacctbal']


class QueryRuntimeConstants:
    # max memory for each executor
    MAX_MEMORY = '10G'

    # number of worker in Spark
    NUM_WORKER = 1

    # entry point for your application
    ENTRY_CLASS = 'ruiliu.aqp.tpch.QueryTPCH'

    # master node url for the cluster
    MASTER = 'spark://southport:7077'

    # supply configuration for jre
    JAVA_OPT = 'spark.executor.extraJavaOptions=-Xms10G -XX:+UseParallelGC -XX:+UseParallelOldGC'

    # entry point for your application
    # ENTRY_JAR = '$SPARK_HOME/jars/ruiliu-aqp_2.11-2.4.0.jar'
    ENTRY_JAR = '/tank/hdfs/ruiliu/rotary-aqp/spark/jars/ruiliu-aqp_2.11-2.4.0.jar'

    # kafka bootstrap server
    BOOTSTRAP_SERVER = 'roscoe:9092'

    # number of batch for input data
    BATCH_NUM = 20

    # number of shuffle for partition
    SHUFFLE_NUM = 20

    # query statistical information
    # STAT_DIR = '/home/run_scripts/stat_dir'
    STAT_DIR = '/tank/hdfs/ruiliu/rotary-aqp/scripts/stat_dir'

    # tpch query static files
    # TPCH_STATIC_DIR = '/home/run_scripts/tpch_static'
    TPCH_STATIC_DIR = '/tank/hdfs/ruiliu/rotary-aqp/scripts/tpch_static'

    # rotary knowledgebase path
    # ROTARY_KNOWLEDGEBASE_PATH = '/home/rotary/knowledgebase'
    ROTARY_KNOWLEDGEBASE_PATH = '/tank/hdfs/ruiliu/rotary-aqp/rotary-aqp/rotary/knowledgebase'

    # scale factor of the input tpch dataset
    SCALE_FACTOR = 1

    # hdfs url for the cluster
    HDFS_ROOT = 'hdfs://southport:9000'

    # 0: querypath-aware
    # 1: subplan-aware
    # 2: IncObv
    # 3: IncStat, collect cardinality groudtruth
    # 4: Run $batch_num, collect selectivities
    EXECUTION_MDOE = 0

    # number of partition for input dataset
    INPUT_PARTITION = 1

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

    # Enable Cost-Based Optimization
    CBO_ENABLE = 'false'

    # trigger interval (milliseconds)
    TRIGGER_INTERVAL = 100

    # aggregation interval (milliseconds)
    AGGREGATION_INTERVAL = 100

    # checkpoint path
    # CHECKPOINT_PATH = 'file:///home/tpch-checkpoint'
    CHECKPOINT_PATH = 'hdfs://southport:9000/tpch-checkpoint'

    # stdout path redirection
    # STDOUT_PATH = '/home/stdout'
    STDOUT_PATH = '/tank/hdfs/ruiliu/rotary-aqp/stdout'

    # stdout path redirection
    # STDERR_PATH = '/home/stderr'
    STDERR_PATH = '/tank/hdfs/ruiliu/rotary-aqp/stderr'

    # saprk work path
    # SPARK_WORK_PATH = '/usr/local/spark/spark-2.4.0-bin-hadoop2.6/work'
    SPARK_WORK_PATH = '/tank/hdfs/ruiliu/rotary-aqp/spark/work'

