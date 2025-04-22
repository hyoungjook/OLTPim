from common import *

EXP_NAME = 'breakdown'
WORKLOADS = ['YCSB-B']
WORKLOAD_SIZES = [10 ** 9]
# (system, no_numa_local, no_interleave, suffix)
BREAKDOWNS = [
    (MOSAICDB, True, False, None),
    (MOSAICDB, False, False, None),
    (OLTPIM, True, True, '_indexonly_nodirect'),
    (OLTPIM, True, True, '_indexonly'),
    (OLTPIM, True, False, '_indexonly'),
    (OLTPIM, True, False, None),
    (OLTPIM, False, False, None),
]
BENCH_SECONDS = 60
BENCH_THREADS = 64
HUGETLB_SIZE_GB = 180

if __name__ == "__main__":
    args = parse_args()
    create_result_file(args, EXP_NAME)
    print_header(args)
    for workload in WORKLOADS:
        for workload_size in WORKLOAD_SIZES:
            for system, no_numa_local, no_interleave, suffix in BREAKDOWNS:
                run(args, system, workload, workload_size, 
                    BENCH_SECONDS, BENCH_THREADS, HUGETLB_SIZE_GB,
                    no_numa_local_workload=no_numa_local,
                    no_interleave=no_interleave,
                    executable_suffix=suffix)
