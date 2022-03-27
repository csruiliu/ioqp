import argparse

from runtime_rotary import RotaryRuntime
from runtime_roundrobin import RoundRobinRuntime
from runtime_relaqs import ReLAQSRuntime
from runtime_heuristic import HeuristicRuntime
from workload.workload_builder import WorkloadBuilder
from common.constants import WorkloadConstants
from common.query_utils import query_memory_fetcher


def arg_config():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--scheduler", action="store", type=str, default="rotary",
                        choices=["rotary", "relaqs", "laf", "edf", "roundrobin"],
                        help="the scheduler mechanism")
    args = parser.parse_args()

    return vars(args)


def main():
    args = arg_config()

    scheduler_name = args["scheduler"]

    workload_builder = WorkloadBuilder(WorkloadConstants.WORKLOAD_SIZE,
                                       WorkloadConstants.WORKLOAD_LIGHT,
                                       WorkloadConstants.WORKLOAD_MEDIUM,
                                       WorkloadConstants.WORKLOAD_HEAVY,
                                       WorkloadConstants.DEADLINE_LIGHT,
                                       WorkloadConstants.DEADLINE_MEDIUM,
                                       WorkloadConstants.DEADLINE_HEAVY,
                                       WorkloadConstants.LIGHT_RATIO,
                                       WorkloadConstants.MEDIUM_RATIO,
                                       WorkloadConstants.HEAVY_RATIO,
                                       WorkloadConstants.ACCURACY_OBJECTIVE,
                                       query_memory_fetcher)

    aqp_workload_dict = workload_builder.generate_workload_aqp(WorkloadConstants.ARRIVAL_LAMBDA,
                                                               WorkloadConstants.SCH_ROUND_PERIOD,
                                                               scheduler_name,
                                                               WorkloadConstants.RANDOM_SEED)

    for job_id, job in aqp_workload_dict.items():
        print(f"job: job_id={job_id}, "
              f"arrive_time={job.arrival_time}, "
              f"deadline={job.deadline}, "
              f"accuracy_threshold={job.accuracy_threshold}, "
              f"schedule_time_window={job.schedule_window}, "
              f"arrived={job.arrive}, "
              f"active={job.active}, "
              f"complete_unattain={job.complete_attain}, "
              f"complete_attain={job.complete_attain}")

    if scheduler_name == "rotary":
        runtime_engine = RotaryRuntime(aqp_workload_dict)
    elif scheduler_name == "roundrobin":
        runtime_engine = RoundRobinRuntime(aqp_workload_dict)
    elif scheduler_name == "relaqs":
        runtime_engine = ReLAQSRuntime(aqp_workload_dict)
    else:
        runtime_engine = HeuristicRuntime(aqp_workload_dict, scheduler_name)

    runtime_engine.run()


if __name__ == "__main__":
    main()
