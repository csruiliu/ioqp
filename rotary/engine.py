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
                               read_aggresult_from_file,
                               read_all_aggresults_from_file,
                               serialize_to_json)

from estimator.rotary_estimator import RotaryEstimator
from estimator.relaqs_estimator import ReLAQSEstimator


class Engine:
    def __init__(self, workload_dict, num_core, num_worker, schedule_slot, scheduler):
        self.workload_dict = workload_dict
        self.num_core = num_core
        self.num_worker = num_worker
        self.schedule_slot = schedule_slot
        self.scheduler = scheduler
        self.batch_size = RuntimeConstants.MAX_STEP // RuntimeConstants.BATCH_NUM
        self.workload_size = len(workload_dict)
        self.estimator_dict = dict()

        #######################################################
        # prepare everything necessary
        #######################################################

        # the dict for counting the job steps (for checkpoint)
        self.job_step_dict = dict()

        # the dict for counting the processing time for each job (excluding checkpoint overhead)
        self.job_processing_time = dict()

        # the dict for storing the aggregation results
        self.job_agg_result_dict = dict()

        # the dict to maintain estimated progress for next epoch for each job
        self.job_estimate_progress = dict()

        # the list stores the jobs ranked by estimated progress and is refreshed every round
        self.priority_queue = list()

        # the list stores the jobs that have arrived but haven't completed, sorted by their arrival time
        self.active_queue = list()

        # the list stores the jobs that have been completed and attained the objective
        self.complete_attain_set = set()

        # the list stores the jobs that have been completed and didn't attain the objective
        self.complete_unattain_set = set()

        for job_id, job_item in self.workload_dict.items():
            self.job_step_dict[job_id] = 0

            self.job_estimate_progress[job_id] = 0.0
            self.job_agg_result_dict[job_id] = list()
            # create an estimator for each job
            if self.scheduler == "rotary":
                self.estimator_dict[job_id] = RotaryEstimator(job_id,
                                                              self.get_agg_schema(job_id),
                                                              self.schedule_slot)
            elif self.scheduler == "relaqs":
                self.estimator_dict[job_id] = ReLAQSEstimator(job_id,
                                                              self.get_agg_schema(job_id),
                                                              self.schedule_slot,
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

    def run_job_slot(self, job_process, stdout_file, stderr_file):
        try:
            job_process.communicate(timeout=self.schedule_slot)
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

        job_parameter_dict = dict()
        job_parameter_dict['job_id'] = job_id
        job_parameter_dict['batch_size'] = self.batch_size
        job_parameter_dict['scale_factor'] = RuntimeConstants.SCALE_FACTOR
        job_parameter_dict['num_worker'] = RuntimeConstants.NUM_WORKER
        job_parameter_dict['agg_interval'] = RuntimeConstants.AGGREGATION_INTERVAL

        for schema_name in agg_schema_list:
            agg_results_dict = read_aggresult_from_file(app_stdout_file, agg_schema_list)
            agg_schema_result = agg_results_dict.get(schema_name)[0]
            agg_schema_current_time = agg_results_dict.get(schema_name)[1]

            job_estimator.epoch_time = agg_schema_current_time
            job_estimator.input_agg_schema_results(agg_schema_result)

            if self.scheduler == "rotary":
                # estimator for rotary
                schema_progress_estimate = job_estimator.predict_progress_next_epoch(job_parameter_dict, schema_name)
            else:
                # estimator for relaqs
                schema_progress_estimate = job_estimator.predict_progress_next_epoch(schema_name)

            job_overall_progress += schema_progress_estimate

        return job_overall_progress / len(agg_schema_list)

    def rank_job(self):
        # clean the priority queue for next round
        self.priority_queue.clear()

        # rank the jobs in active queue according to the estimated
        for job_id in self.active_queue:
            job_stdout_file = RuntimeConstants.STDOUT_PATH + job_id + '.stdout'
            self.job_step_dict[job_id] = read_curstep_from_file(job_stdout_file)
            self.job_estimate_progress[job_id] = self.compute_progress_next_epoch(job_id)

            for k, v in sorted(self.job_estimate_progress.items(), key=lambda x: x[1], reverse=True):
                self.priority_queue.append(k)

    def check_job_completeness(self):
        for job_id in self.active_queue:
            job = self.workload_dict[job_id]

            if job.time_elapse >= job.deadline:
                job.complete_unattain = True
                print(f"the job {job_id} is completed but not attained")
                self.complete_unattain_set.add(job_id)
                self.active_queue.remove(job_id)
            else:
                print(f"the job {job_id} stay in active, has run {job.time_elapse} seconds")

    def process_job(self):
        prerun_time_start = time.perf_counter()

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

            prerun_time_end = time.perf_counter()

            for sp, sp_out, sp_err in subprocess_list:
                # run the job for a scheduling slot
                self.run_job_slot(sp, sp_out, sp_err)

            # compute the pre-run time
            prerun_time = prerun_time_end - prerun_time_start
            self.time_elapse(prerun_time + self.schedule_slot)

            # check the job completeness
            self.check_job_completeness()

            # rank the jobs for next scheduling round
            self.rank_job()

        else:
            subprocess_list = list()
            for jidx in np.arange(self.num_core):
                job_id = self.active_queue[jidx]
                job = self.workload_dict[job_id]

                self.active_queue.remove(job_id)
                self.active_queue.append(job_id)

                subp, out_file, err_file = self.create_job(job, resource_unit=1)
                subprocess_list.append((subp, out_file, err_file))

            prerun_time_end = time.perf_counter()

            for sp, sp_out, sp_err in subprocess_list:
                # run the job for a scheduling slot
                self.run_job_slot(sp, sp_out, sp_err)

            # compute the pre-run time
            prerun_time = prerun_time_end - prerun_time_start
            self.time_elapse(prerun_time + self.schedule_slot)

            # check the job completeness
            self.check_job_completeness()

            # rank the jobs for next scheduling round
            self.rank_job()

    def check_arrived_job(self):
        for job_id, job in self.workload_dict.items():
            if job.arrived and not job.activated:
                print(f"the job {job_id} arrives and is activated")
                self.active_queue.append(job_id)
                job.activated = True
                self.workload_dict[job_id] = job

    def time_elapse(self, time_period):
        # the time unit is second
        for job_id, job in self.workload_dict.items():
            job.move_forward(time_period)
            self.workload_dict[job_id] = job

    def run(self):
        while len(self.complete_attain_set) + len(self.complete_unattain_set) != self.workload_size:
            self.check_arrived_job()

            if self.active_queue:
                # overall_process_time = self.fake_process_job()
                # print(f"time elapse: {overall_process_time}")
                # self.time_elapse(overall_process_time)
                # self.fake_check_complete_job()
                self.process_job()
            else:
                self.time_elapse(1)

    def fake_process_job(self):
        if self.num_core >= len(self.active_queue):
            for job_id in self.active_queue:
                job = self.workload_dict[job_id]
                if np.random.random() > 0.5:
                    job.complete_attain = True
                    self.workload_dict[job_id] = job
                else:
                    if job.time_elapse >= job.deadline:
                        job.complete_unattain = True
                        self.workload_dict[job_id] = job

        return np.random.randint(1, 10)

    def fake_check_complete_job(self):
        for job_id in self.active_queue:
            job = self.workload_dict[job_id]

            if job.complete_attain:
                print(f"the job {job_id} is completed and attained, runtime: {job.time_elapse} seconds")
                self.complete_attain_set.add(job_id)
                self.active_queue.remove(job_id)
            elif job.complete_unattain:
                print(f"the job {job_id} is completed but not attained, runtime: {job.time_elapse} seconds")
                self.complete_unattain_set.add(job_id)
                self.active_queue.remove(job_id)
            else:
                print(f"the job {job_id} stay in active, has run {job.time_elapse} seconds")

    def test(self):
        app_id = read_appid_from_file("/home/stdout/q1.stdout")
        app_stdout_file = RuntimeConstants.SPARK_WORK_PATH + '/' + app_id + '/0/stdout'

        para_dict = dict()
        para_dict['batch_size'] = self.batch_size
        para_dict['scale_factor'] = RuntimeConstants.SCALE_FACTOR
        para_dict['num_core'] = 1
        para_dict['agg_interval'] = RuntimeConstants.AGGREGATION_INTERVAL

        agg_results_dict = read_all_aggresults_from_file(app_stdout_file, RuntimeConstants.Q1_AGG_COL, para_dict)

        agg_results_dict_list = list()
        agg_results_dict_list.append(agg_results_dict)

        serialize_to_json("q1", agg_results_dict)

        rotary_estimator = RotaryEstimator("q1", RuntimeConstants.Q1_AGG_COL, 5)
