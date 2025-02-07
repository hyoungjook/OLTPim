from common import *

EXP_NAME = 'ycsb'
# (workload, size, gc, logging, zipfian, oltpim_only)
TEST_CASES = [
    ('YCSB-C', [10**6, 10**7, 10**8, 10**9], True, True, False, False),
    ('YCSB-B', [10**6, 10**7, 10**8, 10**9], True, True, [False, True], False),
    ('YCSB-A', [10**6, 10**7, 10**8, 10**9], [False, True], True, False, False),
    (['YCSB-I1', 'YCSB-I2', 'YCSB-I3', 'YCSB-I4'], 10**8, True, True, False, False),
    (['YCSB-I1', 'YCSB-I2', 'YCSB-I3', 'YCSB-I4'], 10**8, True, False, False, True),
    (['YCSB-U1', 'YCSB-U2', 'YCSB-U3', 'YCSB-U4'], 10**8, True, [False, True], False, True),
    (['YCSB-S2', 'YCSB-S4', 'YCSB-S8', 'YCSB-S16'], 10**8, True, True, False, False),
]
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
    for workloads, sizes, gcs, loggings, zipfians, oltpim_only in TEST_CASES:
        if not isinstance(workloads, list): workloads = [workloads]
        if not isinstance(sizes, list): sizes = [sizes]
        if not isinstance(gcs, list): gcs = [gcs]
        if not isinstance(loggings, list): loggings = [loggings]
        if not isinstance(zipfians, list): zipfians = [zipfians]
        systems = [OLTPIM] if oltpim_only else [MOSAICDB, OLTPIM]
        for workload in workloads:
            for size in sizes:
                for gc in gcs:
                    for logging in loggings:
                        for zipfian in zipfians:
                            for system in systems:
                                run(args, system, workload, size,
                                    BENCH_SECONDS(workload, size), BENCH_THREADS,
                                    HUGETLB_SIZE_GB,
                                    coro_batch_size=COROBATCH_SIZE(system, workload),
                                    no_gc=(not gc), no_logging=(not logging),
                                    ycsb_zipfian=zipfian)
