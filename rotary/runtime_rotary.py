import os
import time
import psutil
import math
import copy
import signal
import subprocess
import numpy as np
import multiprocessing as mp
from pathlib import Path

from estimator.rotary_estimator import RotaryEstimator
from estimator.envelop_bounder import EnvelopBounder
from workload.job_aqp import JobAQP
from common.loggers import get_logger_instance
from common.constants import QueryRuntimeConstants, WorkloadConstants
from common.file_utils import read_appid_from_file, read_aggresult_from_file
from common.query_utils import generate_job_cmd, agg_schema_fetcher, query_memory_fetcher


class RotaryRuntime:
    def __init__(self, workload_dict):

        self.workload_dict = workload_dict
        self.workload_size = len(workload_dict)
        self.available_cpu_core = mp.cpu_count() // 2 - 2

        self.num_worker = QueryRuntimeConstants.NUM_WORKER
        self.schedule_time_window = WorkloadConstants.SCH_ROUND_PERIOD
        self.check_time_window = WorkloadConstants.CHECK_PERIOD
        self.ckpt_offset = WorkloadConstants.CKPT_PERIOD

        # create a logger
        self.logger = get_logger_instance()

        #######################################################
        # global data structure
        #######################################################

        # the list stores the jobs that are active but haven't completed, sorted by their arrival time
        self.active_queue = list()

        # the list stores the jobs for extra resources and is refreshed every epoch
        self.priority_queue = list()

        # the list stores the jobs for progress checking, it is updated for each basic schedule time unit
        self.check_queue = list()

        # for debug, the list stores the jobs are running for the current epoch
        self.running_queue = list()

        # the list stores the jobs that have been completed and attained the objective
        self.complete_attain_set = set()

        # the list stores the jobs that have been completed and didn't attain the objective
        self.complete_unattain_set = set()

        """
        The dict for envelop bounder for each schema of each job to decide when to stop a job
        key: job_id 
        value: a dict to store the envelop bounder instance for a schema name. 
               This key of this dict is schema name, the value is envelop bounder instance   
        """
        self.job_envelop_dict = dict()

        """
        The dict to store the job process running from background 
        key: job_id
        value: a tuple for process handler, stdout handler, stderr handler
        """
        self.job_process_dict = dict()

        """
        The dict to store the job process running from background 
        key: job_id
        value: the number of cores has been allocated for current scheduling epoch
        """
        self.job_resource_dict = dict()

        """
        The dict to store the job process running from background 
        key: job_id
        value: the number of epochs that each job has run
        """
        self.job_epoch_dict = dict()

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

        # the list stores the summary results of each job
        self.final_result_msg = list()

        # the list stores the accuracy of each job over epochs
        self.job_overall_agg_dict = dict()

        #######################################################
        # data structure for rotary
        #######################################################

        """
        The dict for estimator of each job 
        key: job_id 
        value: estimator instance for each job   
        """
        self.job_estimator_dict = dict()

        """
        The dict to maintain estimated progress for next epoch for each job
        key: job_id
        value: the estimated progress for next epoch
        """
        self.job_estimate_progress = dict()

        #######################################################
        # data structure for laf
        #######################################################
        """
        The list to store the job_id according to the rank of accuracy (least first)
        """
        workload_dict_rank = sorted(self.workload_dict.values(), key=lambda x: x.accuracy_threshold)
        self.job_accuracy_rank = [x.job_id for x in workload_dict_rank]

        #######################################################
        # data structure for edf
        #######################################################
        """
        The dict to store the job_id with time left according to deadline
        """
        self.job_time_left = dict()

        #######################################################
        # initialization data structures
        #######################################################

        for job_id, job_item in self.workload_dict.items():
            self.job_resource_dict[job_id] = 0
            self.job_epoch_dict[job_id] = 0
            self.job_estimate_progress[job_id] = 0.0

            # init time left to deadline for each job
            self.job_time_left[job_id] = job_item.deadline

            self.job_agg_result_dict[job_id] = dict()
            self.job_agg_time_dict[job_id] = dict()
            self.job_envelop_dict[job_id] = dict()
            self.job_overall_agg_dict[job_id] = list()

            # init lists for all schemas for each job
            for schema_name in agg_schema_fetcher(job_id):
                self.job_agg_result_dict[job_id][schema_name] = list()
                self.job_agg_time_dict[job_id][schema_name] = list()
                self.job_envelop_dict[job_id][schema_name] = EnvelopBounder(seq_length=3)

            rotary_estimator = RotaryEstimator(job_id, agg_schema_fetcher(job_id), self.schedule_time_window)
            rotary_estimator.import_knowledge_archive(QueryRuntimeConstants.ROTARY_KNOWLEDGEBASE_PATH,
                                                      WorkloadConstants.WORKLOAD_FULL)
            self.job_estimator_dict[job_id] = rotary_estimator

    def check_arrived_job(self):
        for job_id, job in self.workload_dict.items():
            if job.arrive and not job.active and not job.complete_attain and not job.complete_unattain:
                self.logger.info(f"the job {job_id} arrives and is active now")
                self.active_queue.append(job_id)
                job.active = True
                self.workload_dict[job_id] = job

    def run_job(self, job_id, resource_unit):
        self.running_queue.append(job_id)
        job: JobAQP = self.workload_dict[job_id]
        job.running = True
        self.workload_dict[job_id] = job
        self.logger.info(f"== Start to run {job_id} for epoch {self.job_epoch_dict[job_id]} ==")

        job_output_id = job_id + "-" + str(self.job_epoch_dict[job_id])
        stdout_file = open(QueryRuntimeConstants.STDOUT_PATH + "/" + job_output_id + ".stdout", "w+")
        stderr_file = open(QueryRuntimeConstants.STDERR_PATH + '/' + job_output_id + '.stderr', "w+")

        job_cmd = generate_job_cmd(resource_unit, job_id)
        time.sleep(1)
        subp = subprocess.Popen(job_cmd,
                                stdout=stdout_file,
                                stderr=stderr_file,
                                start_new_session=True)

        return subp, stdout_file, stderr_file

    def process_active_queue(self):
        # check available cpu cores
        if self.available_cpu_core == 0:
            self.logger.info("no available cpu resources for allocation")
            return

        # check available memory
        available_mem = psutil.virtual_memory().available / math.pow(1024, 3)

        active_queue_copy = copy.deepcopy(self.active_queue)

        # more resources than active jobs
        if self.available_cpu_core >= len(self.active_queue):
            extra_cores = self.available_cpu_core - len(self.active_queue)

            # check if the job in the priority queue, if so provide 2 cores otherwise 1
            if self.priority_queue:
                if len(self.priority_queue) > extra_cores:
                    for jidx in np.arange(extra_cores):
                        job_id = self.priority_queue[jidx]
                        if available_mem > query_memory_fetcher(job_id):
                            subp, out_file, err_file = self.run_job(job_id, resource_unit=2)
                            available_mem = available_mem - query_memory_fetcher(job_id)
                            self.job_resource_dict[job_id] = 2
                            self.available_cpu_core -= 2
                            self.job_process_dict[job_id] = (subp, out_file, err_file)
                            self.active_queue.remove(job_id)
                            active_queue_copy.remove(job_id)
                        else:
                            self.logger.info(f"Job {job_id} cannot start since there is no enough memory")
                else:
                    for job_id in self.priority_queue:
                        if available_mem > query_memory_fetcher(job_id):
                            subp, out_file, err_file = self.run_job(job_id, resource_unit=2)
                            available_mem = available_mem - query_memory_fetcher(job_id)
                            self.job_resource_dict[job_id] = 2
                            self.available_cpu_core -= 2
                            self.job_process_dict[job_id] = (subp, out_file, err_file)
                            self.active_queue.remove(job_id)
                            active_queue_copy.remove(job_id)
                        else:
                            self.logger.info(f"Job {job_id} cannot start since there is no enough memory")

            for job_id in self.active_queue:
                if available_mem > query_memory_fetcher(job_id):
                    subp, out_file, err_file = self.run_job(job_id, resource_unit=1)
                    available_mem = available_mem - query_memory_fetcher(job_id)
                    self.job_resource_dict[job_id] = 1
                    self.available_cpu_core -= 1
                    self.job_process_dict[job_id] = (subp, out_file, err_file)
                    active_queue_copy.remove(job_id)
                else:
                    self.logger.info(f"Job {job_id} cannot start since there is no enough memory")

        # less resources than active jobs
        else:
            for jidx in np.arange(self.available_cpu_core):
                job_id = self.active_queue[jidx]
                if available_mem > query_memory_fetcher(job_id):
                    subp, out_file, err_file = self.run_job(job_id, resource_unit=1)
                    available_mem = available_mem - query_memory_fetcher(job_id)
                    self.job_resource_dict[job_id] = 1
                    self.available_cpu_core -= 1
                    self.job_process_dict[job_id] = (subp, out_file, err_file)
                    active_queue_copy.remove(job_id)
                else:
                    self.logger.info(f"Job {job_id} cannot start since there is no enough memory")

        self.active_queue = active_queue_copy.copy()

    def time_elapse(self, time_period):
        # the time unit is second
        for job_id, job in self.workload_dict.items():
            job.move_forward(time_period)
            self.workload_dict[job_id] = job

    def check_progress(self):
        for job_id, (job_proc, out_file, err_file) in self.job_process_dict.items():
            job = self.workload_dict[job_id]

            if job.check:
                self.logger.info(f"Job {job_id} hits time window")
                out_file.close()
                err_file.close()
                # job_proc.terminate()
                os.killpg(job_proc.pid, signal.SIGTERM)

                job.check = False
                job.running = False
                job.active = True
                # reset the job scheduling time window plus checkpoint offset
                job.reset_scheduling_window_progress(self.ckpt_offset)
                self.available_cpu_core += self.job_resource_dict[job_id]
                self.job_resource_dict[job_id] = 0

                self.active_queue.append(job_id)
                self.check_queue.append(job_id)
                self.running_queue.remove(job_id)
            else:
                if job.running:
                    self.logger.info(f"Job {job_id} is running, not hitting time window")

            self.workload_dict[job_id] = job

    def collect_results(self):
        for job_id in self.check_queue:
            job_epoch = str(self.job_epoch_dict[job_id])
            # finish an epoch and collect results so add 1
            self.job_epoch_dict[job_id] += 1
            shell_output = QueryRuntimeConstants.STDOUT_PATH + "/" + job_id + "-" + job_epoch + ".stdout"
            app_id = read_appid_from_file(shell_output)
            app_stdout_file = QueryRuntimeConstants.SPARK_WORK_PATH + '/' + app_id + '/0/stdout'
            app_stdout_path = Path(app_stdout_file)

            if app_stdout_path.is_file():
                agg_schema_list = agg_schema_fetcher(job_id)
                current_agg_results_dict = read_aggresult_from_file(app_stdout_path, agg_schema_list)

                # extract and store the agg result and time
                for schema_name in agg_schema_list:
                    # store agg result
                    self.job_agg_result_dict[job_id][schema_name].append(current_agg_results_dict[schema_name][0])
                    # store agg time
                    self.job_agg_time_dict[job_id][schema_name].append(current_agg_results_dict[schema_name][1])
                    # update envelop function
                    schema_envelop_function: EnvelopBounder = self.job_envelop_dict[job_id][schema_name]
                    schema_envelop_function.input_agg_result(current_agg_results_dict[schema_name][0])
                    self.job_envelop_dict[job_id][schema_name] = schema_envelop_function

    def check_completeness(self):
        for job_id in self.check_queue:
            job: JobAQP = self.workload_dict[job_id]

            # calculate the average estimated accuracy/progress
            schema_estimate_agg_sum = 0
            agg_schema_list = agg_schema_fetcher(job_id)
            for schema_name in agg_schema_list:
                envelop_func: EnvelopBounder = self.job_envelop_dict[job_id][schema_name]
                job_estimated_accuracy = envelop_func.get_estimated_accuracy()
                schema_estimate_agg_sum += job_estimated_accuracy
            job_average_estimated_accuracy = schema_estimate_agg_sum / len(agg_schema_list)

            self.job_overall_agg_dict[job_id].append(job_average_estimated_accuracy)

            self.logger.info(f"Job {job_id} estimated accuracy {job_average_estimated_accuracy}")

            if job.accuracy_threshold < job_average_estimated_accuracy:
                job.complete_attain = True
                job.active = False
                final_msg = (f"Job {job_id} is completed at {self.job_epoch_dict[job_id]} and attained, " +
                             f"running time:{job.run_time}, wait time:{job.wait_time}, " +
                             f"ckpt time:{job.checkpoint_time}, accuracy track: {self.job_overall_agg_dict[job_id]}")
                self.logger.info(final_msg)
                self.final_result_msg.append(final_msg)
                self.complete_attain_set.add(job_id)
                self.active_queue.remove(job_id)
                self.check_queue.remove(job_id)
            elif job.overall_time >= job.deadline:
                job.complete_unattain = True
                job.active = False
                final_msg = (f"Job {job_id} is completed at {self.job_epoch_dict[job_id]} and attained, " +
                             f"running time:{job.run_time}, wait time:{job.wait_time}, " +
                             f"ckpt time:{job.checkpoint_time}, accuracy track: {self.job_overall_agg_dict[job_id]}")
                self.logger.info(final_msg)
                self.final_result_msg.append(final_msg)
                self.complete_unattain_set.add(job_id)
                self.active_queue.remove(job_id)
                self.check_queue.remove(job_id)
            else:
                self.logger.info(f"the job {job_id} stay in active, has run {job.overall_time} seconds")

            self.workload_dict[job_id] = job

    def compute_progress_next_epoch(self, job_id):
        job_epoch = str(self.job_epoch_dict[job_id])
        shell_output = QueryRuntimeConstants.STDOUT_PATH + "/" + job_id + "-" + job_epoch + ".stdout"
        app_id = read_appid_from_file(shell_output)

        app_stdout_file = QueryRuntimeConstants.SPARK_WORK_PATH + '/' + app_id + '/0/stdout'
        agg_schema_list = agg_schema_fetcher(job_id)

        job_estimator = self.job_estimator_dict[job_id]

        job_overall_progress = 0

        job_parameter_dict = dict()
        job_parameter_dict['job_id'] = job_id
        job_parameter_dict['scale_factor'] = QueryRuntimeConstants.SCALE_FACTOR
        job_parameter_dict['num_worker'] = QueryRuntimeConstants.NUM_WORKER
        job_parameter_dict['agg_interval'] = QueryRuntimeConstants.AGGREGATION_INTERVAL

        for schema_name in agg_schema_list:
            agg_results_dict = read_aggresult_from_file(app_stdout_file, agg_schema_list)
            agg_schema_result = agg_results_dict.get(schema_name)[0]
            agg_schema_current_time = agg_results_dict.get(schema_name)[1]

            job_estimator.epoch_time = agg_schema_current_time
            job_estimator.input_agg_schema_results(agg_schema_result)
            schema_progress_estimate = job_estimator.predict_progress_next_epoch(job_parameter_dict, schema_name)
            job_overall_progress += schema_progress_estimate

        return job_overall_progress / len(agg_schema_list)

    def rank_job_next_epoch(self):
        # compute the estimated progress of jobs in the active queue
        for job_id in self.active_queue:
            self.job_estimate_progress[job_id] = self.compute_progress_next_epoch(job_id)

        for k, v in sorted(self.job_estimate_progress.items(), key=lambda x: x[1], reverse=True):
            self.priority_queue.append(k)

    def run(self):
        # if STDOUT_PATH or STDERR_PATH doesn't exist, create them then
        if not Path(QueryRuntimeConstants.STDOUT_PATH).is_dir():
            Path(QueryRuntimeConstants.STDOUT_PATH).mkdir()
        if not Path(QueryRuntimeConstants.STDERR_PATH).is_dir():
            Path(QueryRuntimeConstants.STDERR_PATH).mkdir()

        while len(self.complete_attain_set) + len(self.complete_unattain_set) != self.workload_size:
            self.logger.info("#####################################################################")
            self.check_arrived_job()

            if self.active_queue or self.running_queue:
                self.logger.info(f"** Active Queue ** {self.active_queue}")
                self.logger.info(f"** Priority Queue ** {self.priority_queue}")

                # start to process arriving jobs
                self.process_active_queue()
                # show the running jobs
                self.logger.info(f"** Running Queue {self.running_queue} **")
                # let the jobs run for a time window plus checkpoint read overhead
                time.sleep(self.check_time_window)
                # make the time elapse for schedule_time_window
                self.time_elapse(self.check_time_window)

                # reset the check queue and priority queue for each epoch
                self.priority_queue.clear()
                self.check_queue.clear()

                # check the progress within a unit of time window
                self.check_progress()
                # review check queue
                self.logger.info(f"** Check Queue ** {self.check_queue}")
                # collect results
                self.collect_results()
                # check the job completeness
                self.check_completeness()
                # rank the jobs for next scheduling epoch
                self.rank_job_next_epoch()

            else:
                self.logger.info("No job arrives")
                time.sleep(1)
                self.time_elapse(1)

        for msg in self.final_result_msg:
            self.logger.info(msg)
