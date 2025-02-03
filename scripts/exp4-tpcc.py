from common import *

EXP_NAME = 'tpcc'
WORKLOADS = ['TPC-C']
WORKLOAD_SIZES = [1024]

# (system, corobatch_size, threads)
TEST_CASES = [
    (MOSAICDB, 8, 8),
    (OLTPIM, 512, 8),
    (MOSAICDB, 8, 16),
    (OLTPIM, 256, 16),
    (MOSAICDB, 8, 32),
    (OLTPIM, 128, 32),
    (MOSAICDB, 8, 64),
    (OLTPIM, 64, 64),
]
GC = [False, True]

BENCH_SECONDS = 30
HUGETLB_SIZE_GB = 180

if __name__ == "__main__":
    args = parse_args()
    create_result_file(args, EXP_NAME)
    print_header(args)
    for workload in WORKLOADS:
        for workload_size in WORKLOAD_SIZES:
            for gc in GC:
                for system, corobatch_size, threads in TEST_CASES:
                    run(args, system, workload, workload_size,
                        BENCH_SECONDS, threads, HUGETLB_SIZE_GB,
                        coro_batch_size=corobatch_size,
                        no_gc=(not gc))
