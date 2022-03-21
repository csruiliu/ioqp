import numpy as np


class EnvelopBounder:
    def __init__(self, seq_length=4):
        self._upper_bound = np.NINF
        self._lower_bound = np.inf
        self._seq_length = seq_length

        self._agg_list = list()

    def input_agg_result(self, agg_result):
        if len(self.agg_list) >= self._seq_length:
            # remove the first item in the current agg_list
            self.agg_list.pop(0)

        # add the latest item to the current agg_list
        self.agg_list.append(agg_result)

        self._upper_bound = max(self.agg_list)
        self._lower_bound = min(self.agg_list)

    def get_estimated_accuracy(self):
        if len(self.agg_list) < self._seq_length:
            return 0

        if self._upper_bound == 0:
            return 0

        return self._lower_bound / self._upper_bound

    @property
    def agg_list(self):
        return self._agg_list

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
