import signal
import os
import subprocess
import numpy as np
from pathlib import Path
from common.file_utils import (read_curstep_from_file,
                               read_appid_from_file,
                               read_aggresult_from_file)


class Scheduler:
    def __init__(self, workload, num_cores, slot, constants, estimator):
        self.workload = workload
        self.workload_size = len(workload)
        self.num_cores = num_cores
        self.schedule_slot = slot
        self.runtime_constants = constants
        self.estimator = estimator

        #######################################################
        # prepare everything necessary
        #######################################################

        # self.job_runtime_dict = dict()
        self.job_step_dict = dict()
        self.job_agg_result_dict = dict()

        for job_item in self.workload:
            job_item_key = job_item.job_id
            self.job_step_dict[job_item_key] = 0
            self.job_agg_result_dict[job_item_key] = list()

    @staticmethod
    def delete_dir(dir_name):
        if Path(dir_name).is_dir():
            for f in Path(dir_name).iterdir():
                if f.is_file():
                    f.unlink()

            Path(dir_name).rmdir()

    def get_agg_schema(self, job_id):
        if job_id.startswith('q1'):
            return self.runtime_constants.Q1_AGG_COL
        elif job_id.startswith('q3'):
            return self.runtime_constants.Q3_AGG_COL
        elif job_id.startswith('q5'):
            return self.runtime_constants.Q5_AGG_COL
        elif job_id.startswith('q6'):
            return self.runtime_constants.Q6_AGG_COL
        elif job_id.startswith('q11'):
            return self.runtime_constants.Q11_AGG_COL
        elif job_id.startswith('q16'):
            return self.runtime_constants.Q16_AGG_COL
        elif job_id.startswith('q19'):
            return self.runtime_constants.Q19_AGG_COL
        else:
            ValueError('The query is not supported')

    def generate_job_cmd(self, res_unit, job_name):
        command = ('$SPARK_HOME/bin/spark-submit' +
                   f' --total-executor-cores {res_unit}' +
                   f' --executor-memory {self.runtime_constants.MAX_MEMORY}' +
                   f' --class {self.runtime_constants.ENTRY_CLASS}' +
                   f' --master {self.runtime_constants.MASTER}' +
                   f' --conf "{self.runtime_constants.JAVA_OPT}"' +
                   f' {self.runtime_constants.ENTRY_JAR}' +
                   f' {self.runtime_constants.BOOTSTRAP_SERVER}' +
                   f' {job_name}' +
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

        return command

    def create_job(self, job, resource_unit):
        self.generate_job_cmd(resource_unit, job.job_id)
        stdout_file = open(self.runtime_constants.STDOUT_PATH + '/' + job.job_id + '.stdout', "a+")
        stderr_file = open(self.runtime_constants.STDERR_PATH + '/' + job.job_id + '.stderr', "a+")

        job_cmd = self.generate_job_cmd(resource_unit, job.job_id)
        subp = subprocess.Popen(job_cmd,
                                bufsize=0,
                                stdout=stdout_file,
                                stderr=stderr_file,
                                shell=True)

        return subp, stdout_file, stderr_file

    def stop_job(self, job_process, stdout_file, stderr_file):
        try:
            job_process.communicate(timeout=self.schedule_slot)
        except subprocess.TimeoutExpired:
            stdout_file.close()
            stderr_file.close()
            os.killpg(os.getpgid(job_process.pid), signal.SIGTERM)
            job_process.terminate()

    def process_job_trial(self):
        # if STDOUT_PATH or STDERR_PATH doesn't exist, create them then
        if not Path(self.runtime_constants.STDOUT_PATH).is_dir():
            Path(self.runtime_constants.STDOUT_PATH).mkdir()
        if not Path(self.runtime_constants.STDERR_PATH).is_dir():
            Path(self.runtime_constants.STDERR_PATH).mkdir()

        # more resource than workload size
        if self.num_cores >= len(self.workload):
            resource_unit = self.num_cores // self.workload_size
            subprocess_list = list()
            for job in self.workload:
                subp, out_file, err_file = self.create_job(job, resource_unit)
                subprocess_list.append((subp, out_file, err_file))

            for sp, sp_out, sp_err in subprocess_list:
                self.stop_job(sp, sp_out, sp_err)


            for job in self.workload:
                job_id = job.job_id
                job_stdout_file = self.runtime_constants.STDOUT_PATH + job_id + '.stdout'
                self.job_step_dict[job_id] = read_curstep_from_file(job_stdout_file)
                app_id = read_appid_from_file(job_id + '.stdout')

                app_stdout_file = self.runtime_constants.SPARK_WORK_PATH + '/' + app_id + '/0/stdout'
                agg_schema_list = self.get_agg_schema(job_id)

                for schema_name in agg_schema_list:
                    agg_results_dict = read_aggresult_from_file(app_stdout_file, agg_schema_list)
                    agg_results_dict.get(schema_name)
                    

                # self.job_runtime_dict[job_name] =


        # less resource than workload size
        else:
            resource_unit = 1
            subprocess_list = list()

            trial_num = self.workload_size // self.num_cores
            trial_rest = self.workload_size % self.num_cores

            for tidx in np.arange(trial_num):
                for jidx in np.arange(self.num_cores):
                    job = self.workload[tidx+jidx]
                    subp, out_file, err_file = self.create_job(job, resource_unit)
                    subprocess_list.append((subp, out_file, err_file))

                for sp, sp_out, sp_err in subprocess_list:
                    self.stop_job(sp, sp_out, sp_err)

                for jidx in np.arange(self.num_cores):
                    job = self.workload[tidx + jidx]
                    job_name = job.job_id
                    self.job_step_dict[job_name] = read_curstep_from_file(job_name + '.stdout')

            if trial_rest != 0:
                for jidx in np.arange(trial_rest):
                    job = self.workload[trial_num * self.num_cores + jidx]
                    subp, out_file, err_file = self.create_job(job, resource_unit)
                    subprocess_list.append((subp, out_file, err_file))

                for sp, sp_out, sp_err in subprocess_list:
                    self.stop_job(sp, sp_out, sp_err)

                for jidx in np.arange(trial_rest):
                    job = self.workload[trial_num * self.num_cores + jidx]
                    job_name = job.job_id
                    self.job_step_dict[job_name] = read_curstep_from_file(job_name + '.stdout')

    def process_job(self):
        selected_jobs = self.estimator.predict()

    def run(self):
        # process each job for one schedule slot
        self.process_job_trial()

    def output(self):
        pass
