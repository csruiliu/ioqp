import numpy as np
from .job_aqp import JobAQP


class WorkloadBuilder:
    def __init__(self,
                 workload_size,
                 light_job_list,
                 medium_job_list,
                 heavy_job_list,
                 light_job_deadline_list,
                 medium_job_deadline_list,
                 heavy_job_deadline_list,
                 light_ratio,
                 medium_ratio,
                 heavy_ratio,
                 accuracy_objective_list,
                 memory_fetcher):

        self.workload_size = workload_size
        self.light_job_list = light_job_list
        self.medium_job_list = medium_job_list
        self.heavy_job_list = heavy_job_list
        self.light_job_deadline_list = light_job_deadline_list
        self.medium_job_deadline_list = medium_job_deadline_list
        self.heavy_job_deadline_list = heavy_job_deadline_list
        self.light_ratio = light_ratio
        self.medium_ratio = medium_ratio
        self.heavy_ratio = heavy_ratio
        self.accuracy_objective_list = accuracy_objective_list
        self.memory_fetcher = memory_fetcher

    def generate_workload_aqp(self, arrival_lamda, sch_time_scalar, random_seed):
        num_light_job = int(self.workload_size * self.light_ratio)
        num_medium_job = int(self.workload_size * self.medium_ratio)
        num_heavy_job = int(self.workload_size * self.heavy_ratio)

        assert num_light_job + num_medium_job + num_heavy_job == self.workload_size

        workload = dict()

        np.random.seed(random_seed)
        arrival_time_list = list(np.random.poisson(arrival_lamda, size=self.workload_size))

        np.random.seed(random_seed)
        accuracy_threshold_list = list(np.random.choice(self.accuracy_objective_list, self.workload_size))

        np.random.seed(random_seed)
        light_deadline_list = list(np.random.choice(self.light_job_deadline_list, num_light_job))

        np.random.seed(random_seed)
        medium_deadline_list = list(np.random.choice(self.medium_job_deadline_list, num_medium_job))

        np.random.seed(random_seed)
        heavy_deadline_list = list(np.random.choice(self.heavy_job_deadline_list, num_heavy_job))

        deadline_list = light_deadline_list + medium_deadline_list + heavy_deadline_list

        np.random.seed(random_seed)
        light_query_id_list = list(np.random.choice(self.light_job_list, num_light_job))

        np.random.seed(random_seed)
        medium_query_id_list = list(np.random.choice(self.medium_job_list, num_medium_job))

        np.random.seed(random_seed)
        heavy_query_id_list = list(np.random.choice(self.heavy_job_list, num_heavy_job))

        query_id_list = light_query_id_list + medium_query_id_list + heavy_query_id_list

        # shuffle two list using the same order
        np.random.seed(random_seed)
        zip_list = list(zip(deadline_list, query_id_list))
        np.random.shuffle(zip_list)
        deadline_list, query_id_list = zip(*zip_list)

        for i in np.arange(0, self.workload_size):
            query_id = query_id_list[i]
            job_id = query_id + '_' + str(i)

            sch_period = self.memory_fetcher(query_id) / 5 * sch_time_scalar

            job = JobAQP(job_id,
                         arrival_time_list[i] * 60,
                         accuracy_threshold_list[i],
                         deadline_list[i],
                         sch_period)

            workload[job_id] = job

        return workload
