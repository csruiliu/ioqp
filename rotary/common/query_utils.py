from .constants import MemoryConstants, TPCHAGGConstants, QueryRuntimeConstants
from .file_utils import read_memory_consumption_from_file


def agg_schema_fetcher(job_id):
    query_id = job_id.split("_")[0]
    if query_id == "q1":
        return TPCHAGGConstants.Q1_AGG_COL
    elif query_id == "q2":
        return TPCHAGGConstants.Q2_AGG_COL
    elif query_id == "q3":
        return TPCHAGGConstants.Q3_AGG_COL
    elif query_id == "q4":
        return TPCHAGGConstants.Q4_AGG_COL
    elif query_id == "q5":
        return TPCHAGGConstants.Q5_AGG_COL
    elif query_id == "q6":
        return TPCHAGGConstants.Q6_AGG_COL
    elif query_id == "q7":
        return TPCHAGGConstants.Q7_AGG_COL
    elif query_id == "q8":
        return TPCHAGGConstants.Q8_AGG_COL
    elif query_id == "q9":
        return TPCHAGGConstants.Q9_AGG_COL
    elif query_id == "q10":
        return TPCHAGGConstants.Q10_AGG_COL
    elif query_id == "q11":
        return TPCHAGGConstants.Q11_AGG_COL
    elif query_id == "q12":
        return TPCHAGGConstants.Q12_AGG_COL
    elif query_id == "q13":
        return TPCHAGGConstants.Q13_AGG_COL
    elif query_id == "q14":
        return TPCHAGGConstants.Q14_AGG_COL
    elif query_id == "q15":
        return TPCHAGGConstants.Q15_AGG_COL
    elif query_id == "q16":
        return TPCHAGGConstants.Q16_AGG_COL
    elif query_id == "q17":
        return TPCHAGGConstants.Q17_AGG_COL
    elif query_id == "q18":
        return TPCHAGGConstants.Q18_AGG_COL
    elif query_id == "q19":
        return TPCHAGGConstants.Q19_AGG_COL
    elif query_id == "q20":
        return TPCHAGGConstants.Q20_AGG_COL
    elif query_id == "q21":
        return TPCHAGGConstants.Q21_AGG_COL
    elif query_id == "q22":
        return TPCHAGGConstants.Q22_AGG_COL
    else:
        raise ValueError('The query is not supported')


def query_memory_fetcher(job_id):
    file_path = QueryRuntimeConstants.STDOUT_PATH + "/" + job_id + "-memory.stdout"
    memory = read_memory_consumption_from_file(file_path)
    return memory


def generate_job_cmd(res_unit, job_name):
    command = list()

    max_mem = query_memory_fetcher(job_name)
    java_opt = "spark.executor.extraJavaOptions=-Xms" + str(max_mem) + "G -XX:+UseParallelGC -XX:+UseParallelOldGC"
    command.append("/tank/hdfs/ruiliu/rotary-aqp/spark/bin/spark-submit")
    command.append("--master")
    command.append(QueryRuntimeConstants.MASTER)
    command.append("--class")
    command.append(QueryRuntimeConstants.ENTRY_CLASS)
    command.append("--total-executor-cores")
    command.append(f"{res_unit}")
    command.append("--executor-memory")
    command.append(f"{max_mem}G")
    command.append("--conf")
    command.append(f"{java_opt}")
    command.append(f"{QueryRuntimeConstants.ENTRY_JAR}")
    command.append(f"{QueryRuntimeConstants.BOOTSTRAP_SERVER}")
    command.append(f"{job_name}")
    command.append(f"{QueryRuntimeConstants.BATCH_NUM}")
    command.append(f"{QueryRuntimeConstants.SHUFFLE_NUM}")
    command.append(f"{QueryRuntimeConstants.STAT_DIR}")
    command.append(f"{QueryRuntimeConstants.TPCH_STATIC_DIR}")
    command.append(f"{QueryRuntimeConstants.SCALE_FACTOR}")
    command.append(f"{QueryRuntimeConstants.HDFS_ROOT}")
    command.append(f"{QueryRuntimeConstants.EXECUTION_MDOE}")
    command.append(f"{QueryRuntimeConstants.INPUT_PARTITION}")
    command.append(f"{QueryRuntimeConstants.CONSTRAINT}")
    command.append(f"{QueryRuntimeConstants.LARGEDATASET}")
    command.append(f"{QueryRuntimeConstants.XXXX}")
    command.append(f"{QueryRuntimeConstants.INC_PERCENTAGE}")
    command.append(f"{QueryRuntimeConstants.COST_BIAS}")
    command.append(f"{QueryRuntimeConstants.MAX_STEP}")
    command.append(f"{QueryRuntimeConstants.SAMPLE_TIME}")
    command.append(f"{QueryRuntimeConstants.SAMPLE_RATIO}")
    command.append(f"{QueryRuntimeConstants.TRIGGER_INTERVAL}")
    command.append(f"{QueryRuntimeConstants.AGGREGATION_INTERVAL}")
    command.append(f"{QueryRuntimeConstants.CHECKPOINT_PATH}")
    command.append(f"{QueryRuntimeConstants.CBO_ENABLE}")

    return command