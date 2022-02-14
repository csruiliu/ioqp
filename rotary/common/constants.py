
class RotaryConstants:
    # knowledgebase path
    KNOWLEDGEBASE_PATH = '/home/rotary/knowledgebase'

    # query list
    QUERY_LIST = ['q1', 'q3', 'q5', 'q6', 'q11', 'q16', 'q19']

    # full query list
    QUERY_LIST_FULL = ["q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8" "q9", "q10", "q11", "q12",
                       "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22"]


class TPCHConstants:
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


class RuntimeConstants:
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
    AGGREGATION_INTERVAL = 100

    # checkpoint path
    CHECKPOINT_PATH = 'file:///home/tpch-checkpoint'

    # stdout path redirection
    STDOUT_PATH = '/home/stdout'

    # stdout path redirection
    STDERR_PATH = '/home/stderr'

    # saprk work path
    SPARK_WORK_PATH = '/usr/local/spark/spark-2.4.0-bin-hadoop2.6/work'
