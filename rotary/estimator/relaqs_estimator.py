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

        self._epoch_time = 0

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
            self.agg_runtime_dict[schema_name].append(self._epoch_time)
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

        progress_estimation = self.func_progress((self._epoch_time + self.schedule_slot), popt[0], popt[1])

        return progress_estimation

    @property
    def epoch_time(self):
        return self._epoch_time

    @epoch_time.setter
    def epoch_time(self, value):
        self._epoch_time = value
