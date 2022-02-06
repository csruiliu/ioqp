import json
import numpy as np
from pathlib import Path


def delete_dir(dir_name):
    if Path(dir_name).is_dir():
        for f in Path(dir_name).iterdir():
            if f.is_file():
                f.unlink()

        Path(dir_name).rmdir()


def read_curstep_from_file(file_path):
    for line in reversed(open(file_path).readlines()):
        if 'currentStep' in line:
            cur_step = line.split()[1]
            return cur_step


def read_appid_from_file(file_path):
    for line in open(file_path).readlines():
        if 'APPID' in line:
            application_id = line.split(':')[1]
            return application_id.strip()


def read_aggresult_from_file(file_path, target_schema_list):
    """
        get latest agg results from stdout file according to the target schema list
        the results are stored in a dict
        key: schema name
        value: [current agg result/value, current time]
    """
    agg_result_dict = dict()
    agg_indicator_list = [0] * len(target_schema_list)

    for schema_name in target_schema_list:
        agg_result_dict[schema_name] = [0.0, 0]

    for line in reversed(open(file_path).readlines()):
        agg_tag = 'Aggregation|'

        if agg_tag in line:
            agg_list = line.strip().split('|')
            agg_schema = agg_list[1]
            agg_result = float(agg_list[2])
            agg_current_time = int(agg_list[3])

            if agg_schema in target_schema_list:
                if agg_current_time > agg_result_dict[agg_schema][1]:
                    agg_result_dict[agg_schema][0] = agg_result
                    agg_result_dict[agg_schema][1] = agg_current_time
                    agg_indicator_list[target_schema_list.index(agg_schema)] = 1

            if 0 not in agg_indicator_list:
                return agg_result_dict


def read_all_aggresults_from_file(file_path, target_schema_list, parameter_dict):
    """
        get all agg results from stdout file according to the target schema list
        the results are stored in a dict
        key: schema name
        value: [(agg result1, time1), (agg result2, time2),...,]
    """
    agg_result_dict = dict()
    agg_result_dict_json = dict()

    agg_interval = parameter_dict['agg_interval']

    agg_start_time = np.inf

    for schema in target_schema_list:
        agg_result_dict[schema] = list()
        agg_result_dict_json[schema] = dict()

    for line in open(file_path).readlines():
        agg_tag = 'Aggregation|'

        if agg_tag in line:
            agg_list = line.strip().split('|')
            agg_schema = agg_list[1]
            agg_result = float(agg_list[2])
            agg_current_time = int(agg_list[3])

            if agg_current_time < agg_start_time:
                agg_start_time = agg_current_time

            if agg_schema in target_schema_list:
                agg_result_dict[agg_schema].append((agg_result, agg_current_time - agg_start_time))

    for k, v in agg_result_dict.items():
        agg_previous_time = v[0][1]
        agg_slice_count = 1
        agg_slice_sum: float = v[0][0]

        agg_results_list = list()
        agg_time_list = list()

        for item in v[1:]:
            agg_result = item[0]
            agg_current_time = item[1]
            if agg_current_time - agg_previous_time > (agg_interval // 2):
                # agg_result_dict_clean[k].append(((agg_slice_sum / agg_slice_count), agg_previous_time))
                agg_results_list.append(agg_slice_sum / agg_slice_count)
                agg_time_list.append(agg_previous_time)
                agg_slice_count = 1
                agg_slice_sum = agg_result
                agg_previous_time = agg_current_time
            elif agg_previous_time == agg_current_time:
                agg_slice_count += 1
                agg_slice_sum += agg_result
            else:
                print("ignore the item due to the wrong time sequence")

        agg_result_dict_json[k]['result'] = agg_results_list
        agg_result_dict_json[k]['time'] = agg_time_list

        for para, value in parameter_dict.items():
            agg_result_dict_json[para] = value

    return agg_result_dict_json


def serialize_to_json(query_name, input_dict_list):
    with open("/home/rotary/knowledgebase/" + query_name + ".json", "w+") as outfile:
        json.dump(input_dict_list, outfile)


def list_to_json_file():
    json_str = '[{"results":[1,2,3], "runtime":[2,3,4]}]'
    alist = json.loads(json_str)
    print(alist[0]["results"])


def main():
    list_to_json_file()


if __name__ == "__main__":
    main()
