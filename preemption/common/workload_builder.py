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

    def generate_workload_aqp(self):
        workload = list()
        for i in np.arange(1, self.workload_size+1):
            job = JobAQP(np.random.choice(self.job_list, 1)[0] + '_' + str(i),
                         np.random.choice(self.accuracy_list, 1)[0],
                         np.random.choice(self.deadline_list, 1)[0])

            workload.append(job)

        return workload

