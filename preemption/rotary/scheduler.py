import subprocess


class Rotary:
    def __init__(self, workload, num_cores, slot, constants):
        self.workload = workload
        self.workload_size = len(workload)
        self.num_cores = num_cores
        self.schedule_slot = slot
        self.runtime_constants = constants

    def run_init(self):
        # more resource than workload
        if self.num_cores >= len(self.workload):
            resource_unit = self.num_cores // self.workload_size
            subprocess_list = list()
            for job in self.workload:
                job_cmd = ('$SPARK_HOME/bin/spark-submit' +
                           f' --total-executor-cores {resource_unit}' +
                           f' --executor-memory {self.runtime_constants.MAX_MEMORY}' +
                           f' --class {self.runtime_constants.ENTRY_CLASS}' +
                           f' --master {self.runtime_constants.MASTER}' +
                           f' --conf "{self.runtime_constants.JAVA_OPT}"' +
                           f' {self.runtime_constants.ENTRY_JAR}' +
                           f' {self.runtime_constants.BOOTSTRAP_SERVER}' +
                           f' {job.name}' +
                           f' {self.runtime_constants.BATCH_NUM}' +
                           f' {self.runtime_constants.SHUFFLE_NUM}' +
                           f' {self.runtime_constants.STAT_DIR}' +
                           f' {self.runtime_constants.TPCH_STATIC_DIR}' +
                           f' {self.runtime_constants.SCALE_FACTOR}' +
                           f' {self.runtime_constants.HDFS_ROOT}' +
                           f' {self.runtime_constants.EXECUTION_MDOE}' +
                           f' {self.runtime_constants.INPUT_PARTITION}' +
                           f' {self.runtime_constants.CONSTRAINT}' +
                           f' {self.runtime_constants.LARGEDATASET}' +
                           f' {self.runtime_constants.IOLAP}' +
                           f' {self.runtime_constants.INC_PERCENTAGE}' +
                           f' {self.runtime_constants.COST_BIAS}' +
                           f' {self.runtime_constants.MAX_STEP}' +
                           f' {self.runtime_constants.SAMPLE_TIME}' +
                           f' {self.runtime_constants.SAMPLE_RATIO}' +
                           f' {self.runtime_constants.TRIGGER_INTERVAL}' +
                           f' {self.runtime_constants.AGGREGATION_INTERVAL}' +
                           f' {self.runtime_constants.CHECKPOINT_PATH}')

                subp = subprocess.Popen(job_cmd,
                                        bufsize=0,
                                        shell=True)

                subprocess_list.append(subp)
                # out, _ = subp.communicate()

            for sp in subprocess_list:
                sp.wait()

        else:
            resource_unit = 1

    def run(self):
        self.run_init()
