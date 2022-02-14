import numpy as np
from .job_aqp import JobAQP


class WorkloadBuilder:
    def __init__(self, workload_size, job_list, accuracy_list, deadline_list):
        self.workload_size = workload_size
        if isinstance(job_list, list):
            self.job_list = job_list
        else:
            raise TypeError('the job candidates should be a list')

        if isinstance(accuracy_list, list):
            self.accuracy_list = accuracy_list
        else:
            raise TypeError('the accuracy threshold candidates should be a list')

        if isinstance(deadline_list, list):
            self.deadline_list = deadline_list
        else:
            raise TypeError('the deadline candidates should be a list')

    def generate_workload_aqp(self, arrival_lamda, random_seed):
        workload = dict()

        np.random.seed(random_seed)
        arrival_time_list = np.random.poisson(arrival_lamda, size=self.workload_size)

        np.random.seed(random_seed)
        accuracy_threshold_list = np.random.choice(self.accuracy_list, self.workload_size)

        np.random.seed(random_seed)
        deadline_list = np.random.choice(self.deadline_list, self.workload_size)

        np.random.seed(random_seed)
        job_id_list = np.random.choice(self.job_list, self.workload_size)

        for i in np.arange(0, self.workload_size):
            job_id = job_id_list[i] + '_' + str(i)
            job = JobAQP(job_id,
                         arrival_time_list[i],
                         accuracy_threshold_list[i],
                         deadline_list[i])

            workload[job_id] = job

        return workload
