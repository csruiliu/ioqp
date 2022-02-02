
def read_curstep_from_file(file_path):
    for line in reversed(open(file_path).readlines()):
        if 'currentStep' in line:
            cur_step = line.split()[1]
            return cur_step


def read_appid_from_file(file_path):
    for line in open(file_path).readlines():
        if 'APPID' in line:
            app_id = line.split(':')[1]
            return app_id.strip()


def read_aggresult_from_file(file_path, target_schema_list):
    agg_result_dict = dict()
    agg_indicator_list = [0] * len(target_schema_list)

    for schema_name in target_schema_list:
        agg_result_dict[schema_name] = [0.0, 0]

    for line in reversed(open(file_path).readlines()):
        agg_tag = 'Aggregation|'

        if agg_tag in line:
            agg_list = line.split('|')
            agg_schema = agg_list[1]
            print(agg_list[2])
            agg_result = float(agg_list[2])
            agg_current_time = int(agg_list[3])

            if agg_schema in target_schema_list:
                if agg_current_time > agg_result_dict[agg_schema][1]:
                    agg_result_dict[agg_schema][0] = agg_result
                    agg_result_dict[agg_schema][1] = agg_current_time
                    agg_indicator_list[target_schema_list.index(agg_schema)] = 1

            if 0 not in agg_indicator_list:
                return agg_result_dict


if __name__ == "__main__":
    stdout_file = '/home/stdout/q1_1.stdout'

    read_curstep_from_file(stdout_file)
    app_id = read_appid_from_file(stdout_file)
    print(app_id)

    spark_work = '/usr/local/spark/spark-2.4.0-bin-hadoop2.6/work'

    agg_result, agg_last_time = read_aggresult_from_file(spark_work + '/' + app_id + '/0/stdout', 'avg_qty')

    print(agg_result)
    print(agg_last_time)
