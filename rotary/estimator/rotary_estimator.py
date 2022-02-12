import json
import numpy as np
from pathlib import Path


class RotaryEstimator:
    def __init__(self, job_id, schema_list, schedule_slot, topk=5, poly_deg=3):
        self.job_id = job_id
        self.schema_list = schema_list
        self.schedule_slot = schedule_slot

        self.agg_results_dict = dict()
        self.agg_runtime_dict = dict()
        self.agg_progress_dict = dict()

        for schema_name in schema_list:
            self.agg_results_dict[schema_name] = list()
            self.agg_runtime_dict[schema_name] = list()
            self.agg_progress_dict[schema_name] = list()

        self._epoch_time = 0
        self._top_k = topk
        self._deg = poly_deg

        """
        The dict for storing archived knowledge files
        key: query_id
        value: a list of archived knowledge for each query id. Each archived knowledge is a dict. 
               Each dict has parameters and agg results and time
        """
        self._knowledge_dict_archive = dict()

        self._knowledge_dict_realtime_result = dict()
        self._knowledge_dict_realtime_runtime = dict()

    def input_agg_schema_results(self, input_dict):
        for schema_name, schema_value in input_dict.items():
            self.agg_results_dict[schema_name].append(schema_value[0])
            self.agg_runtime_dict[schema_name].append(schema_value[1])

    def import_knowledge_archive(self, knowledgebase_path, query_list):
        kb_path = Path(knowledgebase_path)
        if not kb_path.is_dir():
            raise ValueError("The knowledgebase path doesn't exist")

        for query_id in query_list:
            path_list = kb_path.glob(query_id + "*.json")
            self._knowledge_dict_archive[query_id] = list()
            for kb_file in path_list:
                with open(kb_file) as ka:
                    kb_archive_dict = json.load(ka)
                    self._knowledge_dict_archive[query_id].append(kb_archive_dict)

    def import_knowledge_realtime(self, input_dict):
        for schema_name, schema_value in input_dict.items():
            self._knowledge_dict_realtime_result[schema_name].append(schema_value[0])
            self._knowledge_dict_realtime_runtime[schema_name].append(schema_value[1])

    def compute_archive_similarity(self, center_point, candidate_points, schema_name):
        # TODO: need a sophisticated mechanism for similarity computation
        similarity_list = list()
        topk_point_list = list()

        for candidate in candidate_points:
            """ 
                We only take the candidate archived dataset that has: 
                1. same dataset (e.g., TPCH)
                2. same schema name
                3. same scale factor
                4. same batch size
                5. same number of worker
                6. same aggregation interval
                Otherwise, set the similarity as -1
            """
            if (center_point['batch_size'] == candidate['batch_size'] and
                    center_point['scale_factor'] == candidate['scale_factor'] and
                    center_point['num_worker'] == candidate['num_worker'] and
                    center_point['agg_interval'] == candidate['agg_interval']):

                topk_point_list.append(candidate[schema_name])

            else:
                similarity_list.append(-1)

        if len(topk_point_list) > self._top_k:
            return np.random.shuffle(topk_point_list)[:self._top_k]
        else:
            return np.random.shuffle(topk_point_list)

    def predict_progress_next_epoch(self, job_parameters, schema_name):
        """
            :parameter
            schema_name: name of schema for prediction
            input_x: the x of a linear model for prediction, e.g., accuracy or epoch
            mode: predicting 'accuracy' or 'epoch'

            if predicting accuracy, input_x is $epoch, mode='accuracy'.
            if predicting epoch, input_x is $accuracy, mode='epoch'.
        """
        query_id = job_parameters['job_id'].split('_')[0]

        selected_archive_list = self.compute_archive_similarity(job_parameters,
                                                                self._knowledge_dict_archive[query_id],
                                                                schema_name)

        # init a new curve weight list for this prediction
        curve_weight_list = list()
        # init a result list for this prediction
        result_list = list()
        # init an epoch list for this prediction
        epoch_list = list()

        # count the archive data point
        archive_dp_num = 0

        for archive_item in selected_archive_list:
            result_list.extend(archive_item['result'])
            epoch_list.extend(archive_item['time'])
            archive_dp_num += len(archive_item['result'])

        result_list.extend(self._knowledge_dict_realtime_result[schema_name])
        epoch_list.extend(self._knowledge_dict_realtime_runtime[schema_name])

        # count the realtime data point
        realtime_dp_num = len(self._knowledge_dict_realtime_result[schema_name])

        realtime_weight = 1 / (realtime_dp_num + 1)
        archive_weight = realtime_weight / archive_dp_num

        curve_weight_list.extend([archive_weight] * archive_dp_num)
        curve_weight_list.extend([realtime_weight] * realtime_dp_num)

        coefs = np.polyfit(x=np.asarray(epoch_list),
                           y=np.asarray(result_list),
                           deg=self._deg,
                           w=curve_weight_list)

        agg_estimate = np.polyval(coefs, self._epoch_time + self.schedule_slot)

        return agg_estimate

    @property
    def epoch_time(self):
        return self._epoch_time

    @epoch_time.setter
    def epoch_time(self, value):
        self._epoch_time = value
