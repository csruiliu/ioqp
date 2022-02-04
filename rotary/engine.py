import os
import time
import copy
import signal
import subprocess
import numpy as np
from pathlib import Path

from common.constants import RuntimeConstants
from common.file_utils import (read_curstep_from_file,
                               read_appid_from_file,
                               read_aggresult_from_file)

from estimator.rotary_estimator import RotaryEstimator
from estimator.relaqs_estimator import ReLAQSEstimator


class Engine:
    def __init__(self, workload_dict, num_core, num_worker, schedule_round, scheduler):
        self.workload_dict = workload_dict
        self.num_core = num_core
        self.num_worker = num_worker
        self.schedule_round = schedule_round
        self.scheduler = scheduler
        self.batch_size = RuntimeConstants.MAX_STEP // RuntimeConstants.BATCH_NUM
        self.workload_size = len(workload_dict)
        self.estimator_dict = dict()

        #######################################################
        # prepare everything necessary
        #######################################################

        self.job_step_dict = dict()
        self.job_agg_result_dict = dict()
        # the dict to maintain estimated progress for next epoch for each job
        self.job_estimate_progress = dict()

        # the list stores the jobs ranked by estimated progress and is refreshed every round
        self.priority_queue = list()

        # the list stores that jobs haven't arrived
        self.inactive_queue = list()

        # the list stores the jobs that have arrived, sorted by their arrival time
        self.active_queue = list()

        # the list stores the jobs that have been completed and attained the objective
        self.complete_attain_queue = list()

        # the list stores the jobs that have been completed and didn't attain the objective
        self.complete_unattain_queue = list()

        for job_id, job_item in self.workload_dict.items():
            self.inactive_queue.append(job_id)
            self.job_step_dict[job_id] = 0
            self.job_estimate_progress[job_id] = 0.0
            self.job_agg_result_dict[job_id] = list()
            # create an estimator for each job
            if self.scheduler == "rotary":
                self.estimator_dict[job_id] = RotaryEstimator(job_id, self.get_agg_schema(job_id))
            elif self.scheduler == "relaqs":
                self.estimator_dict[job_id] = ReLAQSEstimator(job_id,
                                                              self.get_agg_schema(job_id),
                                                              self.schedule_round,
                                                              self.batch_size,
                                                              self.num_worker)
            else:
                raise ValueError("the scheduler is not supported")



    @staticmethod
    def get_agg_schema(job_id):
        if job_id.startswith('q1'):
            return RuntimeConstants.Q1_AGG_COL
        elif job_id.startswith('q3'):
            return RuntimeConstants.Q3_AGG_COL
        elif job_id.startswith('q5'):
            return RuntimeConstants.Q5_AGG_COL
        elif job_id.startswith('q6'):
            return RuntimeConstants.Q6_AGG_COL
        elif job_id.startswith('q11'):
            return RuntimeConstants.Q11_AGG_COL
        elif job_id.startswith('q16'):
            return RuntimeConstants.Q16_AGG_COL
        elif job_id.startswith('q19'):
            return RuntimeConstants.Q19_AGG_COL
        else:
            ValueError('The query is not supported')

    @staticmethod
    def generate_job_cmd(res_unit, job_name):
        command = ('$SPARK_HOME/bin/spark-submit' +
                   f' --total-executor-cores {res_unit}' +
                   f' --executor-memory {RuntimeConstants.MAX_MEMORY}' +
                   f' --class {RuntimeConstants.ENTRY_CLASS}' +
                   f' --master {RuntimeConstants.MASTER}' +
                   f' --conf "{RuntimeConstants.JAVA_OPT}"' +
                   f' {RuntimeConstants.ENTRY_JAR}' +
                   f' {RuntimeConstants.BOOTSTRAP_SERVER}' +
                   f' {job_name}' +
                   f' {RuntimeConstants.BATCH_NUM}' +
                   f' {RuntimeConstants.SHUFFLE_NUM}' +
                   f' {RuntimeConstants.STAT_DIR}' +
                   f' {RuntimeConstants.TPCH_STATIC_DIR}' +
                   f' {RuntimeConstants.SCALE_FACTOR}' +
                   f' {RuntimeConstants.HDFS_ROOT}' +
                   f' {RuntimeConstants.EXECUTION_MDOE}' +
                   f' {RuntimeConstants.INPUT_PARTITION}' +
                   f' {RuntimeConstants.CONSTRAINT}' +
                   f' {RuntimeConstants.LARGEDATASET}' +
                   f' {RuntimeConstants.IOLAP}' +
                   f' {RuntimeConstants.INC_PERCENTAGE}' +
                   f' {RuntimeConstants.COST_BIAS}' +
                   f' {RuntimeConstants.MAX_STEP}' +
                   f' {RuntimeConstants.SAMPLE_TIME}' +
                   f' {RuntimeConstants.SAMPLE_RATIO}' +
                   f' {RuntimeConstants.TRIGGER_INTERVAL}' +
                   f' {RuntimeConstants.AGGREGATION_INTERVAL}' +
                   f' {RuntimeConstants.CHECKPOINT_PATH}')

        return command

    def create_job(self, job, resource_unit):
        self.generate_job_cmd(resource_unit, job.job_id)
        stdout_file = open(RuntimeConstants.STDOUT_PATH + '/' + job.job_id + '.stdout', "a+")
        stderr_file = open(RuntimeConstants.STDERR_PATH + '/' + job.job_id + '.stderr', "a+")

        job_cmd = self.generate_job_cmd(resource_unit, job.job_id)
        subp = subprocess.Popen(job_cmd,
                                bufsize=0,
                                stdout=stdout_file,
                                stderr=stderr_file,
                                shell=True)

        return subp, stdout_file, stderr_file

    def stop_job(self, job_process, stdout_file, stderr_file):
        try:
            job_process.communicate(timeout=self.schedule_round)
        except subprocess.TimeoutExpired:
            stdout_file.close()
            stderr_file.close()
            os.killpg(os.getpgid(job_process.pid), signal.SIGTERM)
            job_process.terminate()

    def compute_progress_next_epoch(self, job_id):
        app_id = read_appid_from_file(job_id + '.stdout')

        app_stdout_file = RuntimeConstants.SPARK_WORK_PATH + '/' + app_id + '/0/stdout'
        agg_schema_list = self.get_agg_schema(job_id)

        job_estimator = self.estimator_dict[job_id]

        job_overall_progress = 0
        for schema_name in agg_schema_list:
            agg_results_dict = read_aggresult_from_file(app_stdout_file, agg_schema_list)
            agg_schema_result = agg_results_dict.get(schema_name)[0]
            agg_schema_current_time = agg_results_dict.get(schema_name)[1]

            job_estimator.epoch_increment()
            job_estimator.input_agg_schema_results(agg_schema_result)
            schema_progress_estimate = job_estimator.predict_progress_next_epoch(schema_name)

            job_overall_progress += schema_progress_estimate

        return job_overall_progress / len(agg_schema_list)

    def process_job(self):
        # if STDOUT_PATH or STDERR_PATH doesn't exist, create them then
        if not Path(RuntimeConstants.STDOUT_PATH).is_dir():
            Path(RuntimeConstants.STDOUT_PATH).mkdir()
        if not Path(RuntimeConstants.STDERR_PATH).is_dir():
            Path(RuntimeConstants.STDERR_PATH).mkdir()

        active_queue_deep_copy = copy.deepcopy(self.active_queue)

        if self.num_core >= len(self.active_queue):
            extra_cores = self.num_core - len(self.active_queue)
            subprocess_list = list()
            if self.priority_queue:
                if len(self.priority_queue) > extra_cores:
                    for jidx in np.arange(extra_cores):
                        job_id = self.priority_queue[jidx]
                        job = self.workload_dict[job_id]
                        active_queue_deep_copy.remove(job_id)
                        subp, out_file, err_file = self.create_job(job, resource_unit=2)
                        subprocess_list.append((subp, out_file, err_file))
                else:
                    for job_id in self.priority_queue:
                        job = self.workload_dict[job_id]
                        active_queue_deep_copy.remove(job_id)
                        subp, out_file, err_file = self.create_job(job, resource_unit=2)
                        subprocess_list.append((subp, out_file, err_file))

            for job_id in active_queue_deep_copy:
                job = self.workload_dict[job_id]
                subp, out_file, err_file = self.create_job(job, resource_unit=1)
                subprocess_list.append((subp, out_file, err_file))

            for sp, sp_out, sp_err in subprocess_list:
                self.stop_job(sp, sp_out, sp_err)

            self.priority_queue.clear()
            for job_id in self.active_queue:
                job_stdout_file = RuntimeConstants.STDOUT_PATH + job_id + '.stdout'
                self.job_step_dict[job_id] = read_curstep_from_file(job_stdout_file)
                self.job_estimate_progress[job_id] = self.compute_progress_next_epoch(job_id)

                for k, v in sorted(self.job_estimate_progress.items(), key=lambda x: x[1], reverse=True):
                    self.priority_queue.append(k)

        else:
            subprocess_list = list()
            for jidx in np.arange(self.num_core):
                job_id = self.active_queue[jidx]
                job = self.workload_dict[job_id]

                self.active_queue.remove(job_id)
                self.active_queue.append(job_id)

                subp, out_file, err_file = self.create_job(job, resource_unit=1)
                subprocess_list.append((subp, out_file, err_file))

                for sp, sp_out, sp_err in subprocess_list:
                    self.stop_job(sp, sp_out, sp_err)

    def check_active_job(self):
        for job_id, job in self.workload_dict.items():
            if job.active:
                self.active_queue.append(job_id)
                self.inactive_queue.remove(job_id)

    def check_complete_job(self):
        for job_id in self.active_queue:
            job = self.workload_dict[job_id]

            if job.complete_attain:
                print('the job is completed and attained')
                self.complete_attain_queue.append(job_id)
                self.active_queue.remove(job_id)
            elif job.complete_unattain:
                print('the job is completed but not attained')
                self.complete_unattain_queue.append(job_id)
                self.active_queue.remove(job_id)
            else:
                print('the job stay in active')

    def time_elapse(self):
        for job in self.workload_dict:
            # time.sleep(self.schedule_round)
            job.move_forward(self.schedule_round)
            job.check_arrival()

    def run(self):
        while len(self.complete_attain_queue) + len(self.complete_unattain_queue) != self.workload_size:
            self.check_active_job()

            if self.active_queue:
                self.process_job()
                self.check_complete_job()
                self.time_elapse()
            else:
                self.time_elapse()

