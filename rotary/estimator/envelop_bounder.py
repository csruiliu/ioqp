import numpy as np


class EnvelopBounder:
    def __init__(self, seq_length=4):
        self._upper_bound = np.NINF
        self._lower_bound = np.inf
        self._seq_length = seq_length

        self.agg_list = list()

    def input_agg_results(self, agg_result):
        if len(self.agg_list) > self._seq_length:
            self.agg_list.pop(0)
            self.agg_list.append(agg_result)

            if self._lower_bound > agg_result:
                self._lower_bound = agg_result

            if self._upper_bound < agg_result:
                self._upper_bound = agg_result

    def get_estimated_accuracy(self):
        if len(self.agg_list) < self._seq_length:
            return 0
        return self._lower_bound / self._upper_bound

    @property
    def upper_bound(self):
        return self._upper_bound

    @upper_bound.setter
    def upper_bound(self, value):
        self._upper_bound = value

    @property
    def lower_bound(self):
        return self._lower_bound

    @lower_bound.setter
    def lower_bound(self, value):
        self._lower_bound = value
