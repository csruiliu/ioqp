

class JobAQP:
    def __init__(self, job_id, arrival_time, accuracy_threshold, deadline):
        self._job_id = job_id
        self._arrival_time = arrival_time
        self._accuracy_threshold = accuracy_threshold
        self._deadline = deadline

        self._current_step = 0
        self._active = False
        self._complete_attain = None
        self._complete_unattain = None

    def check_arrival(self):
        if self.arrival_time <= 0:
            self.active = True
        else:
            self.active = False

    def move_forward(self, time_elapse):
        if not self.active:
            self.arrival_time -= time_elapse

    @property
    def job_id(self):
        return self._job_id

    @job_id.setter
    def job_id(self, value):
        self._job_id = value

    @property
    def arrival_time(self):
        return self._arrival_time

    @arrival_time.setter
    def arrival_time(self, value):
        self._arrival_time = value

    @property
    def accuracy_threshold(self):
        return self._accuracy_threshold

    @accuracy_threshold.setter
    def accuracy_threshold(self, value):
        if value == 0 or value is None:
            raise ValueError("the value is not valid")
        self._accuracy_threshold = value

    @property
    def deadline(self):
        return self._deadline

    @deadline.setter
    def deadline(self, value):
        if value == 0 or value is None:
            raise ValueError("the value is not valid")
        self._deadline = value

    @property
    def current_step(self):
        return self._current_step

    @current_step.setter
    def current_step(self, value):
        if not isinstance(value, int):
            raise ValueError("the value can only be int type")
        self._current_step = value

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, value):
        if not isinstance(value, bool):
            raise ValueError("the value can only be bool type")
        self._active = value

    @property
    def complete_attain(self):
        return self._complete_attain

    @complete_attain.setter
    def complete_attain(self, value):
        if not isinstance(value, bool):
            raise ValueError("the value can only be bool type")
        self._complete_attain = value

    @property
    def complete_unattain(self):
        return self._complete_unattain

    @complete_unattain.setter
    def complete_unattain(self, value):
        if not isinstance(value, bool):
            raise ValueError("the value can only be bool type")
        self._complete_unattain = value
