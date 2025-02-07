from common import *

EXP_NAME = 'ycsb'
WORKLOADS = [
    'YCSB-C', 'YCSB-B', 'YCSB-A',
    'YCSB-I1', 'YCSB-I2', 'YCSB-I3', 'YCSB-I4',
    'YCSB-U1', 'YCSB-U2', 'YCSB-U3', 'YCSB-U4',
    'YCSB-S2', 'YCSB-S4', 'YCSB-S8', 'YCSB-S16',
]
WORKLOAD_SIZES = lambda workload: \
    [10 ** 6, 10 ** 7, 10 ** 8, 10 ** 9] if workload in ['YCSB-C', 'YCSB-B', 'YCSB-A'] \
    else [10 ** 8]
GC_OPTS = lambda workload: [False, True] if workload == 'YCSB-A' else [True]
LOGGING_OPTS = lambda workload: \
    [False, True] if workload.startswith('YCSB-I') or workload.startswith('YCSB-U') else [True]
SYSTEMS = lambda workload: [OLTPIM] if workload.startswith('YCSB-U') else [MOSAICDB, OLTPIM]
COROBATCH_SIZE = lambda system, workload: \
    8 if system == MOSAICDB else (128 if 'YCSB-S' in workload else 256)
BENCH_SECONDS = lambda workload, size: \
    30 if (workload == 'YCSB-A' and size == 10**9) else 60
BENCH_THREADS = 64
HUGETLB_SIZE_GB = 180

if __name__ == "__main__":
    args = parse_args()
    create_result_file(args, EXP_NAME)
    print_header(args)
    for workload in WORKLOADS:
        for workload_size in WORKLOAD_SIZES(workload):
            for gc in GC_OPTS(workload):
                for logging in LOGGING_OPTS(workload):
                    for system in SYSTEMS(workload):
                        run(args, system, workload, workload_size,
                            BENCH_SECONDS(workload, workload_size), BENCH_THREADS,
                            HUGETLB_SIZE_GB,
                            coro_batch_size=COROBATCH_SIZE(system, workload),
                            no_gc=(not gc),
                            no_logging=(not logging))
