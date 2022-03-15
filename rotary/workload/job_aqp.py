

class JobAQP:
    def __init__(self, job_id, arrival_time, accuracy_threshold, deadline, sch_window):
        self._job_id = job_id
        self._arrival_time = arrival_time
        self._accuracy_threshold = accuracy_threshold
        self._deadline = deadline
        self._schedule_window = sch_window

        # count the time since the job is arrived
        self._time_elapse = 0

        self._current_step = 0

        # if the job has arrived
        self._arrive = False

        # if the job is running (in the active queue)
        self._active = False

        # if the job hit its scheduling time window
        self._check = False

        # if the job has been complete and attain the objective
        self._complete_attain = False

        # if the job has been complete but not attain the objective
        self._complete_unattain = False

    def move_forward(self, time_elapse):
        if self.complete_unattain or self.complete_attain:
            return

        if self.arrive and self.active:
            self.time_elapse += time_elapse


        else:
            self.arrival_time -= time_elapse
            if self.arrival_time <= 0:
                self.arrive = True

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
    def schedule_window(self):
        return self._schedule_window

    @schedule_window.setter
    def schedule_window(self, value):
        if not isinstance(value, int):
            raise ValueError("the value can only be integer type, unit second")
        self._schedule_window = value

    @property
    def time_elapse(self):
        return self._time_elapse

    @time_elapse.setter
    def time_elapse(self, value):
        self._time_elapse = value

    @property
    def current_step(self):
        return self._current_step

    @current_step.setter
    def current_step(self, value):
        if not isinstance(value, int):
            raise ValueError("the value can only be int type")
        self._current_step = value

    @property
    def arrive(self):
        return self._arrive

    @arrive.setter
    def arrive(self, value):
        if not isinstance(value, bool):
            raise ValueError("the value can only be bool type")
        self._arrive = value

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, value):
        if not isinstance(value, bool):
            raise ValueError("the value can only be bool type")
        self._active = value

    @property
    def check(self):
        return self._check

    @check.setter
    def check(self, value):
        if not isinstance(value, bool):
            raise ValueError("the value can only be bool type")
        self._check = value

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
