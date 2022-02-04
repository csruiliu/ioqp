import json
import numpy as np
from scipy.optimize import curve_fit


class ReLAQSEstimator:
    def __init__(self, job_id, schema_list, schedule_slot, batch_size, num_worker):
        self.job_id = job_id
        self.schema_list = schema_list
        self.schedule_slot = schedule_slot
        self.batch_size = batch_size
        self.num_worker = num_worker
        self.agg_results_dict = dict()
        self.agg_runtime_dict = dict()
        self.agg_progress_dict = dict()

        for schema_name in schema_list:
            self.agg_results_dict[schema_name] = list()
            self.agg_runtime_dict[schema_name] = list()
            self.agg_progress_dict[schema_name] = list()

        self._epoch = 0

    @staticmethod
    def find_max_difference_pair(input_list):
        if len(input_list) < 2:
            return 0
        else:
            max_difference = -1
            for i in range(len(input_list) - 1):
                difference = input_list[i + 1] - input_list[i]
                if max_difference < difference:
                    max_difference = difference

            return max_difference

    @staticmethod
    def func_progress(x, a, b):
        return 1 / (a * x * x + b)

    def func_runtime(self, x, a, b):
        return a * (x * self.batch_size / self.num_worker) + b

    def fit_progress(self, x, y):
        opt, cov = curve_fit(self.func_progress, x, y)
        return opt, cov

    def fit_runtime(self, x, y):
        opt, cov = curve_fit(self.func_runtime, x, y)
        return opt, cov

    def input_agg_schema_results(self, input_dict):
        for schema_name, schema_value in input_dict.items():
            self.agg_results_dict[schema_name].append(schema_value[0])
            self.agg_runtime_dict[schema_name].append(self._epoch * self.schedule_slot)
        self.update_progress()

    def update_progress(self):
        for schema_name in self.schema_list:
            if len(self.agg_results_dict[schema_name]) < 2:
                self.agg_progress_dict[schema_name].append(0)
            else:
                cur_difference = (self.agg_results_dict[schema_name][-1] - self.agg_results_dict[schema_name][-2])
                max_difference = self.find_max_difference_pair(self.agg_results_dict[schema_name])

                if max_difference == 0:
                    cur_progress = 0
                else:
                    cur_progress = cur_difference / max_difference

                self.agg_progress_dict[schema_name].append(cur_progress)

    def predict_progress_next_epoch(self, schema_name):
        epoch_list = self.agg_runtime_dict[schema_name]
        progress_list = self.agg_progress_dict[schema_name]

        popt, pcov = self.fit_progress(np.asarray(epoch_list), np.asarray(progress_list))

        progress_estimation = self.func_progress((self._epoch+1) * self.schedule_slot, popt[0], popt[1])

        return progress_estimation

    def predict_runtime_next_epoch(self, schema_name):
        epoch_list = self.agg_runtime_dict[schema_name]
        progress_list = self.agg_progress_dict[schema_name]

        popt, pcov = self.fit_runtime(np.asarray(progress_list), np.asarray(epoch_list))

        runtime_estimation = self.func_runtime(self._epoch + 1, popt[0], popt[1])

        return runtime_estimation

    def epoch_increment(self):
        self._epoch += 1

    '''
    def predict(self, input_model_dict, input_x, mode):
        job_key = str(input_model_dict['id']) + '-' + input_model_dict['model']

        if job_key in self._job_predict_dict:
            # if the job_key exists, get the predict info
            job_predict_info = self._job_predict_dict[job_key]
            accuracy_list = job_predict_info['accuracy']
            epoch_list = job_predict_info['epoch']
        else:
            # if this is a new model for prediction, compute and generate the predict info
            job_predict_info = dict()
            accuracy_list = list()
            epoch_list = list()

            for nb_model in self._knowledge_list:
                for aidx, acc in enumerate(nb_model['accuracy']):
                    accuracy_list.append(acc)
                    epoch_list.append(aidx)

            job_predict_info['accuracy'] = accuracy_list
            job_predict_info['epoch'] = epoch_list

            self._job_predict_dict[job_key] = job_predict_info
    
    def import_knowledge_archive(self, knowledge_archive):
        with open(knowledge_archive) as ka:
            for knowledge_item in json.load(ka):
                self._knowledge_list.append(knowledge_item)
    '''

