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

    for schema_name in target_schema_list:
        agg_result_dict[schema_name] = [0.0, 0]

    for line in reversed(open(file_path).readlines()):
        agg_tag = 'Aggregation|'

        if agg_tag in line:
            agg_list = line.strip().split('|')
            agg_schema = agg_list[1]
            agg_result = float(agg_list[2])
            agg_start_time = int(agg_list[3])
            agg_current_time = int(agg_list[4])

            if agg_schema in target_schema_list:
                if agg_current_time > agg_result_dict[agg_schema][1]:
                    agg_result_dict[agg_schema][0] = agg_result
                    agg_result_dict[agg_schema][1] = agg_current_time
                    # agg_indicator_list[target_schema_list.index(agg_schema)] = 1

    return agg_result_dict


def serialize_stdout_to_knowledge(input_file_path,
                                  output_file_path,
                                  target_schema_list,
                                  parameter_dict):
    """
    Serialize the stdout to knowledgebase file
    Get all agg results from stdout file according to the target schema list
    """
    agg_knowledge_dict = dict()

    query_id = parameter_dict["query_id"]
    agg_knowledge_dict["query_id"] = query_id
    agg_knowledge_dict["batch_size"] = parameter_dict["batch_size"]
    agg_knowledge_dict["scale_factor"] = parameter_dict["scale_factor"]
    agg_knowledge_dict["num_worker"] = parameter_dict["num_worker"]
    agg_interval = parameter_dict["agg_interval"]
    agg_knowledge_dict["agg_interval"] = agg_interval

    agg_start_time = np.inf

    agg_result_dict = dict()
    agg_time_dict = dict()
    for schema_name in target_schema_list:
        agg_result_dict[schema_name] = list()
        agg_time_dict[schema_name] = list()

    input_file = Path(input_file_path)
    if not input_file.is_file():
        raise ValueError("stdout file doesn't exist")

    agg_tag = 'Aggregation|'
    for line in open(input_file_path).readlines():
        if agg_tag in line:
            agg_list = line.strip().split('|')
            agg_schema = agg_list[1]
            agg_result = float(agg_list[2])
            agg_current_time = int(agg_list[3])

            if agg_current_time < agg_start_time:
                agg_start_time = agg_current_time

            agg_result_dict[agg_schema].append(agg_result)
            agg_time_dict[agg_schema].append(agg_current_time - agg_start_time)

    # start to clean the raw results
    for schema_name in target_schema_list:
        schema_agg_result_list = list()
        schema_agg_time_list = list()

        agg_slice_sum = agg_result_dict[schema_name][0]
        agg_previous_time = agg_time_dict[schema_name][0]
        agg_slice_count = 1

        for tidx, agg_current_time in enumerate(agg_time_dict[schema_name][1:]):
            agg_result = agg_result_dict[schema_name][tidx]
            if agg_current_time - agg_previous_time > (agg_interval // 2):
                schema_agg_result_list.append(agg_slice_sum / agg_slice_count)
                schema_agg_time_list.append(agg_previous_time)
                agg_slice_count = 1
                agg_slice_sum = agg_result
                agg_previous_time = agg_current_time
            elif agg_previous_time == agg_current_time:
                agg_slice_count += 1
                agg_slice_sum += agg_result
            else:
                print("ignore the item due to the wrong time sequence")

        agg_knowledge_dict[schema_name] = dict()
        agg_knowledge_dict[schema_name]["result"] = schema_agg_result_list
        agg_knowledge_dict[schema_name]["time"] = schema_agg_time_list

    # check the existing files
    path_list = Path(output_file_path).glob(query_id + "*.json")

    with open(output_file_path + "/" + query_id + "-" + str(len(list(path_list)) + 1) + ".json", "w+") as outfile:
        json.dump(agg_knowledge_dict, outfile)

