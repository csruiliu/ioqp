import os
import time
import copy
import signal
import subprocess
import numpy as np
from pathlib import Path

from estimator.rotary_estimator import RotaryEstimator
from estimator.relaqs_estimator import ReLAQSEstimator
from estimator.envelop_bounder import EnvelopBounder
from workload.job_aqp import JobAQP
from common.loggers import get_logger_instance
from common.constants import RuntimeConstants, RotaryConstants, TPCHConstants
from common.file_utils import (read_curstep_from_file,
                               read_appid_from_file,
                               read_aggresult_from_file,
                               serialize_stdout_to_knowledge)


class Runtime:
    def __init__(self, workload_dict, num_core, num_worker, schedule_epoch, scheduler):
        self.workload_dict = workload_dict
        self.num_core = num_core
        self.num_worker = num_worker
        self.schedule_epoch = schedule_epoch
        self.scheduler = scheduler
        self.batch_size = RuntimeConstants.MAX_STEP // RuntimeConstants.BATCH_NUM
        self.workload_size = len(workload_dict)

        # create a logger
        self.logger = get_logger_instance()

        #######################################################
        # prepare everything necessary
        #######################################################

        self.global_epoch_count = 0

        """
        The dict for estimator of each job 
        key: job_id 
        value: estimator instance for each job   
        """
        self.job_estimator_dict = dict()

        """
        The dict for envelop bounder for each schema of each job
        key: job_id 
        value: a dict to store the envelop bounder instance for a schema name. 
               This key of this dict is schema name, the value is envelop bounder instance   
        """
        self.job_envelop_dict = dict()

        """
        The dict for counting the job steps (current step)
        key: job_id
        value: the count of current steps 
        """
        self.job_step_dict = dict()

        """
        The dict for storing the aggregation results
        key: job_id
        value: a dict to store the each schema results for the running epoch (when the job is selected to run)
               With in this subdict, the key is schema_name, and value is a list to store agg results over epochs 
        """
        self.job_agg_result_dict = dict()

        """
        The dict for storing the aggregation time (which is associate with job_agg_result_dict)
        key: job_id
        value: a dict to store the each schema processing time for the running epoch (when the job is selected to run)
               With in this subdict, the key is schema_name, and value is a list to store time over epochs 
        """
        self.job_agg_time_dict = dict()

        """
        The dict to maintain estimated progress for next epoch for each job
        key: job_id
        value: the estimated progress for next epoch
        """
        self.job_estimate_progress = dict()

        """
        The list to store the job_id according to the rank of accuracy (least first)
        """
        workload_dict_rank = sorted(self.workload_dict.values(), key=lambda x: x.accuracy_threshold)
        self.job_accuracy_rank = [x.job_id for x in workload_dict_rank]

        """
        The dict to store the job_id with time left according to deadline
        """
        self.job_time_left = dict()

        # the list stores the jobs ranked by estimated progress and is refreshed every epoch
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
            self.job_agg_result_dict[job_id] = dict()
            self.job_agg_time_dict[job_id] = dict()
            self.job_envelop_dict[job_id] = dict()

            self.job_time_left[job_id] = job_item["deadline"]

            # init lists for all schemas for each job
            for schema_name in self.get_agg_schema(job_id):
                self.job_agg_result_dict[job_id][schema_name] = list()
                self.job_agg_time_dict[job_id][schema_name] = list()
                self.job_envelop_dict[job_id][schema_name] = EnvelopBounder(seq_length=4)

            # create an estimator for each job
            if self.scheduler == "rotary":
                rotary_estimator = RotaryEstimator(job_id, self.get_agg_schema(job_id), self.schedule_epoch)
                rotary_estimator.import_knowledge_archive(RotaryConstants.KNOWLEDGEBASE_PATH,
                                                          RotaryConstants.QUERY_LIST)
                self.job_estimator_dict[job_id] = rotary_estimator
            elif self.scheduler == "relaqs":
                self.job_estimator_dict[job_id] = ReLAQSEstimator(job_id,
                                                                  self.get_agg_schema(job_id),
                                                                  self.schedule_epoch,
                                                                  self.batch_size,
                                                                  self.num_worker)
            else:
                raise ValueError("the scheduler is not supported")

    @staticmethod
    def get_agg_schema(job_id):
        if job_id.startswith('q1'):
            return TPCHConstants.Q1_AGG_COL
        elif job_id.startswith('q2'):
            return TPCHConstants.Q2_AGG_COL
        elif job_id.startswith('q3'):
            return TPCHConstants.Q3_AGG_COL
        elif job_id.startswith('q4'):
            return TPCHConstants.Q4_AGG_COL
        elif job_id.startswith('q5'):
            return TPCHConstants.Q5_AGG_COL
        elif job_id.startswith('q6'):
            return TPCHConstants.Q6_AGG_COL
        elif job_id.startswith('q7'):
            return TPCHConstants.Q7_AGG_COL
        elif job_id.startswith('q8'):
            return TPCHConstants.Q8_AGG_COL
        elif job_id.startswith('q9'):
            return TPCHConstants.Q9_AGG_COL
        elif job_id.startswith('q10'):
            return TPCHConstants.Q10_AGG_COL
        elif job_id.startswith('q11'):
            return TPCHConstants.Q11_AGG_COL
        elif job_id.startswith('q12'):
            return TPCHConstants.Q12_AGG_COL
        elif job_id.startswith('q13'):
            return TPCHConstants.Q13_AGG_COL
        elif job_id.startswith('q14'):
            return TPCHConstants.Q14_AGG_COL
        elif job_id.startswith('q15'):
            return TPCHConstants.Q15_AGG_COL
        elif job_id.startswith('q16'):
            return TPCHConstants.Q16_AGG_COL
        elif job_id.startswith('q17'):
            return TPCHConstants.Q17_AGG_COL
        elif job_id.startswith('q18'):
            return TPCHConstants.Q18_AGG_COL
        elif job_id.startswith('q19'):
            return TPCHConstants.Q19_AGG_COL
        elif job_id.startswith('q20'):
            return TPCHConstants.Q20_AGG_COL
        elif job_id.startswith('q21'):
            return TPCHConstants.Q21_AGG_COL
        elif job_id.startswith('q22'):
            return TPCHConstants.Q22_AGG_COL
        else:
            raise ValueError('The query is not supported')

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

        job_output_id = job.job_id + "-" + str(self.global_epoch_count)
        stdout_file = open(RuntimeConstants.STDOUT_PATH + "/" + job_output_id + ".stdout", "w+")
        stderr_file = open(RuntimeConstants.STDERR_PATH + '/' + job_output_id + '.stderr', "w+")

        job_cmd = self.generate_job_cmd(resource_unit, job.job_id)
        subp = subprocess.Popen(job_cmd,
                                bufsize=0,
                                stdout=stdout_file,
                                stderr=stderr_file,
                                shell=True)

        return subp, stdout_file, stderr_file

    def run_job_epoch(self, job_process, stdout_file, stderr_file):
        try:
            job_process.communicate(timeout=self.schedule_epoch)
        except subprocess.TimeoutExpired:
            stdout_file.close()
            stderr_file.close()
            os.killpg(os.getpgid(job_process.pid), signal.SIGTERM)
            job_process.terminate()

    def compute_progress_next_epoch(self, job_id):
        app_id = read_appid_from_file(job_id + '.stdout')

        app_stdout_file = RuntimeConstants.SPARK_WORK_PATH + '/' + app_id + '/0/stdout'
        agg_schema_list = self.get_agg_schema(job_id)

        job_estimator = self.job_estimator_dict[job_id]

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
            elif self.scheduler == "relaqs":
                # estimator for relaqs
                schema_progress_estimate = job_estimator.predict_progress_next_epoch(schema_name)
            else:
                raise ValueError("The scheduler is not supported")

            job_overall_progress += schema_progress_estimate

        return job_overall_progress / len(agg_schema_list)

    def rank_job_next_epoch(self):
        # clean the priority queue for next round
        self.priority_queue.clear()

        if self.scheduler == "rotary" or self.scheduler == "relaqs":
            # compute the estimated progress of jobs in the active queue
            for job_id in self.active_queue:
                job_output_id = job_id + "-" + str(self.global_epoch_count)
                job_stdout_file = RuntimeConstants.STDOUT_PATH + job_output_id + '.stdout'
                self.job_step_dict[job_id] = read_curstep_from_file(job_stdout_file)
                self.job_estimate_progress[job_id] = self.compute_progress_next_epoch(job_id)

            for k, v in sorted(self.job_estimate_progress.items(), key=lambda x: x[1], reverse=True):
                self.priority_queue.append(k)

        elif self.scheduler == "laf":
            job_mask_id_list = [0] * self.workload_size
            for job_in in self.active_queue:
                job_mask_id_list[self.job_accuracy_rank.index(job_in)] = 1

            for job_mask, job_idx in enumerate(job_mask_id_list):
                if job_mask == 1:
                    self.priority_queue.append(self.job_accuracy_rank[job_idx])

        elif self.scheduler == "edf":
            for job_id in self.active_queue:
                job = self.workload_dict[job_id]
                job_time_left = job.deadline - job.time_elapse
                self.job_time_left[job_id] = job_time_left

            for k, v in sorted(self.job_time_left.items(), key=lambda x: x[1]):
                self.priority_queue.append(k)
        else:
            raise ValueError("The scheduler is not supported")

    def check_job_completeness(self):
        for job_id in self.active_queue:
            job: JobAQP = self.workload_dict[job_id]

            # calculate the average estimated accuracy/progress
            schema_estimate_agg_sum = 0
            agg_schema_list = self.get_agg_schema(job_id)
            for schema_name in agg_schema_list:
                envelop_func: EnvelopBounder = self.job_envelop_dict[job_id][schema_name]
                job_estimated_accuracy = envelop_func.get_estimated_accuracy()
                schema_estimate_agg_sum += job_estimated_accuracy
            job_average_estimated_accuracy = schema_estimate_agg_sum / len(agg_schema_list)

            if job.accuracy_threshold < job_average_estimated_accuracy:
                job.complete_attain = True
                self.logger.info(f"the job {job_id} is completed and attained")
                self.complete_attain_set.add(job_id)
                self.active_queue.remove(job_id)
            elif job.time_elapse >= job.deadline:
                job.complete_unattain = True
                self.logger.info(f"the job {job_id} is completed but not attained")
                self.complete_unattain_set.add(job_id)
                self.active_queue.remove(job_id)
            else:
                self.logger.info(f"the job {job_id} stay in active, has run {job.time_elapse} seconds")

    def collect_results_epoch(self):
        # clean the priority queue for next round
        self.priority_queue.clear()

        for job_id in self.active_queue:
            output_file = RuntimeConstants.STDOUT_PATH + "/" + job_id + "-" + str(self.global_epoch_count) + ".stdout"
            output_path = Path(output_file)
            if output_path.is_file():
                agg_schema_list = self.get_agg_schema(job_id)
                current_agg_results_dict = read_aggresult_from_file(output_file, agg_schema_list)

                # extract and store the agg result and time
                self.job_step_dict[job_id] = read_curstep_from_file(output_file)
                for schema_name in agg_schema_list:
                    # store agg result
                    self.job_agg_result_dict[job_id][schema_name].append(current_agg_results_dict[0])
                    # store agg time
                    self.job_agg_time_dict[job_id][schema_name].append(current_agg_results_dict[1])
                    # update envelop function
                    schema_envelop_function: EnvelopBounder = self.job_envelop_dict[job_id][schema_name]
                    schema_envelop_function.input_agg_result(current_agg_results_dict[0])
                    self.job_envelop_dict[job_id][schema_name] = schema_envelop_function

    def process_job(self):
        # start counting the preparation time
        prerun_time_start = time.perf_counter()

        # if STDOUT_PATH or STDERR_PATH doesn't exist, create them then
        if not Path(RuntimeConstants.STDOUT_PATH).is_dir():
            Path(RuntimeConstants.STDOUT_PATH).mkdir()
        if not Path(RuntimeConstants.STDERR_PATH).is_dir():
            Path(RuntimeConstants.STDERR_PATH).mkdir()

        # create a copy of active queue for resource allocation
        active_queue_deep_copy = copy.deepcopy(self.active_queue)

        # more resources than active jobs
        if self.num_core >= len(self.active_queue):
            extra_cores = self.num_core - len(self.active_queue)
            subprocess_list = list()
            # check if the job in the priority queue, if so provide 2 cores otherwise 1
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

        # less resources than active jobs
        else:
            subprocess_list = list()
            for jidx in np.arange(self.num_core):
                job_id = self.active_queue[jidx]
                job = self.workload_dict[job_id]

                # move the job_id to the end for fairness
                self.active_queue.remove(job_id)
                self.active_queue.append(job_id)

                subp, out_file, err_file = self.create_job(job, resource_unit=1)
                subprocess_list.append((subp, out_file, err_file))

        # end counting the preparation time
        prerun_time_end = time.perf_counter()

        # run the job for an epoch
        for sp, sp_out, sp_err in subprocess_list:
            self.run_job_epoch(sp, sp_out, sp_err)

        # compute the pre-run time
        prerun_time = prerun_time_end - prerun_time_start

        # make the time elapse for prerun_time + schedule_epoch to avoid checkpoint time
        self.time_elapse(prerun_time + self.schedule_epoch)

        # collect current
        self.collect_results_epoch()

        # check the job completeness
        self.check_job_completeness()

        # rank the jobs for next scheduling epoch
        self.rank_job_next_epoch()

    def check_arrived_job(self):
        for job_id, job in self.workload_dict.items():
            if job.arrived and not job.activated:
                self.logger.info(f"the job {job_id} arrives and is activated")
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

            self.global_epoch_count += 1

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
                self.logger.info(f"the job {job_id} is completed and attained, runtime: {job.time_elapse} seconds")
                self.complete_attain_set.add(job_id)
                self.active_queue.remove(job_id)
            elif job.complete_unattain:
                self.logger.info(f"the job {job_id} is completed but not attained, runtime: {job.time_elapse} seconds")
                self.complete_unattain_set.add(job_id)
                self.active_queue.remove(job_id)
            else:
                self.logger.info(f"the job {job_id} stay in active, has run {job.time_elapse} seconds")

    def test(self):
        app_id = read_appid_from_file("/home/stdout/q1.stdout")
        app_stdout_file = RuntimeConstants.SPARK_WORK_PATH + '/' + app_id + '/0/stdout'

        parameter_dict = dict()
        parameter_dict["query_id"] = "q1"
        parameter_dict["scale_factor"] = RuntimeConstants.SCALE_FACTOR
        parameter_dict["agg_interval"] = RuntimeConstants.AGGREGATION_INTERVAL
        parameter_dict["batch_size"] = self.batch_size
        parameter_dict["num_worker"] = RuntimeConstants.NUM_WORKER

        serialize_stdout_to_knowledge(app_stdout_file,
                                      RotaryConstants.KNOWLEDGEBASE_PATH,
                                      TPCHConstants.Q1_AGG_COL,
                                      parameter_dict)

        # rotary_estimator = RotaryEstimator("q1", RuntimeConstants.Q1_AGG_COL, 5)

        # for k, v in self.job_estimator_dict.items():
        #     print(v._knowledge_dict_archive)
