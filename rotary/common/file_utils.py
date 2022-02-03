import json
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
        get agg results from stdout file according to the target schema list
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


def list_to_json_file():
    json_str = '[{"results":[1,2,3], "runtime":[2,3,4]}]'
    alist = json.loads(json_str)
    print(alist[0]["results"])


def main():
    list_to_json_file()


if __name__ == "__main__":
    main()
