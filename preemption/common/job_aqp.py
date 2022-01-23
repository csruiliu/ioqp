

class JobAQP:
    def __init__(self, name, accuracy_threshold, deadline):
        self._name = name
        self._accuracy_threshold = accuracy_threshold
        self._deadline = deadline
        self._current_step = 0

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def accuracy_threshold(self):
        return self._accuracy_threshold

    @accuracy_threshold.setter
    def accuracy_threshold(self, value):
        self._accuracy_threshold = value

    @property
    def deadline(self):
        return self._deadline

    @deadline.setter
    def deadline(self, value):
        self._deadline = value

    @property
    def current_step(self):
        return self._current_step

    @current_step.setter
    def current_step(self, value):
        self._current_step = value
