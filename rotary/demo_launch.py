import argparse

from scheduler.rotary import Rotary
from workload.workload_builder import WorkloadBuilder


def arg_config():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--num_core", action="store", type=int, default=20,
                        help="indicate the number of cpu cores for processing")
    parser.add_argument("-w", "--workload_size", action="store", type=int, default=20,
                        help="indicate the size of aqp workload")
    parser.add_argument("-n", "--num_worker", action="store", type=int, default=1,
                        help="indicate the number of worker for aqp")
    parser.add_argument("-r", "--schedule_round", action="store", type=int, default=1,
                        help="time period of each schedule slot [unit: second]")
    parser.add_argument("-s", "--scheduler", action="store", type=str, default="rotary",
                        choices=["rotary", "relaqs", "laf", "edf"], help="the scheduler mechanism")
    parser.add_argument("-l", "--arrival_lambda", action="store", type=int, default=4,
                        help="""the parameter used to generate arrival time. 
                        Expected number of events occurring in a fixed-time interval, 
                        must be >= 0.""")
    args = parser.parse_args()

    return vars(args)


def main():
    args = arg_config()

    num_core = args["num_core"]
    num_worker = args["num_worker"]
    workload_size = args["workload_size"]
    schedule_round = args["schedule_round"]
    scheduler = args["scheduler"]
    arrival_lambda = args["arrival_lambda"]
    """
    tpch_query_list = ["q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8" "q9", "q10", "q11", "q12",
                       "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22"]
    """
    tpch_query_list = ["q1", "q3", "q5", "q6", "q11", "q16", "q19"]
    accuracy_list = [0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
    deadline_list = [60, 120, 180, 240, 300]

    workload_builder = WorkloadBuilder(workload_size, tpch_query_list, accuracy_list, deadline_list)

    aqp_workload_dict = workload_builder.generate_workload_aqp(arrival_lambda, random_seed=42)

    for job_id, job in aqp_workload_dict.items():
        print(f"job: job_id={job_id}, "
              f"arrive_time={job.arrival_time}, "
              f"deadline={job.deadline}, "
              f"accuracy_threshold={job.accuracy_threshold}, "
              f"current_step={job.current_step}, "
              f"arrived={job.arrived}, "
              f"complete_unattain={job.complete_attain}, "
              f"complete_attain={job.complete_attain}")

    sch_engine = Rotary(aqp_workload_dict, num_core, num_worker, schedule_round, scheduler)

    # sch_engine.run()
    sch_engine.test()


if __name__ == "__main__":
    main()
