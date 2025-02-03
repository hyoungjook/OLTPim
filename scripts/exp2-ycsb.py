from common import *

EXP_NAME = 'ycsb'
SYSTEMS = [MOSAICDB, OLTPIM]
WORKLOADS = [
    'YCSB-C', 'YCSB-B', 'YCSB-A',
    'YCSB-I1', 'YCSB-I2', 'YCSB-I3', 'YCSB-I4',
]
WORKLOAD_SIZES = {
    'YCSB-C': [10 ** 6, 10 ** 7, 10 ** 8, 10 ** 9],
    'YCSB-B': [10 ** 6, 10 ** 7, 10 ** 8, 10 ** 9],
    'YCSB-A': [10 ** 6, 10 ** 7, 10 ** 8, 10 ** 9],
    'YCSB-I1': [10 ** 8],
    'YCSB-I2': [10 ** 8],
    'YCSB-I3': [10 ** 8],
    'YCSB-I4': [10 ** 8],
}
GC_OPTS = {
    'YCSB-C': [True],
    'YCSB-B': [False, True],
    'YCSB-A': [False, True],
    'YCSB-I1': [True],
    'YCSB-I2': [True],
    'YCSB-I3': [True],
    'YCSB-I4': [True],
}
BENCH_SECONDS = lambda workload, size: \
    30 if (workload == 'YCSB-A' and size == 10**9) else 60
BENCH_THREADS = 64
HUGETLB_SIZE_GB = 180

if __name__ == "__main__":
    args = parse_args()
    create_result_file(args, EXP_NAME)
    print_header(args)
    for workload in WORKLOADS:
        for workload_size in WORKLOAD_SIZES[workload]:
            for gc in GC_OPTS[workload]:
                for system in SYSTEMS:
                    run(args, system, workload, workload_size,
                        BENCH_SECONDS(workload, workload_size), BENCH_THREADS,
                        HUGETLB_SIZE_GB,
                        no_gc=(not gc))
