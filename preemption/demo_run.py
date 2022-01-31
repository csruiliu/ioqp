import argparse
from common.workload_builder import WorkloadBuilder
from common.constants import RuntimeConstants
from schduler import Scheduler
from estimator.rotary_estimator import RotaryEstimator
from estimator.relaqs_estimator import ReLAQSEstimator


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--num_core', action='store', type=int, default=4,
                        help='indicate the number of cpu cores for processing')
    parser.add_argument('-w', '--workload_size', action='store', type=int, default=1,
                        help='indicate the size of aqp workload')
    parser.add_argument('-s', '--schedule_slot', action='store', type=int, default=100,
                        help='time period of each schedule slot [unit: second]')
    parser.add_argument('-p', '--preemption', action='store', type=str, default='rotary',
                        help='the preemption mechanism working on ')
    args = parser.parse_args()

    num_core = args.num_core
    workload_size = args.workload_size
    schedule_slot = args.schedule_slot
    preemption_name = args.preemption

    # tpch_query_list = ['q1', 'q3', 'q5', 'q6', 'q11', 'q16', 'q19']
    tpch_query_list = ['q1']
    accuracy_list = [0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
    deadline_list = [60, 120, 180, 240, 300]

    workload_builder = WorkloadBuilder(workload_size, tpch_query_list, accuracy_list, deadline_list)

    aqp_workload = workload_builder.generate_workload_aqp()

    for job in aqp_workload:
        print(f"job: {job.job_id}, {job.deadline}, {job.accuracy_threshold}, {job.current_step}")

    if preemption_name == 'rotary':
        sched_preemption = Scheduler(aqp_workload, num_core, schedule_slot, RuntimeConstants, RotaryEstimator)
    else:
        sched_preemption = Scheduler(aqp_workload, num_core, schedule_slot, RuntimeConstants, ReLAQSEstimator)

    sched_preemption.run()


if __name__ == "__main__":
    main()
