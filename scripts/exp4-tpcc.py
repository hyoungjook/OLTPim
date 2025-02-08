from common import *

EXP_NAME = 'tpcc'
WORKLOAD_SIZE = 1024

# (system, workload, corobatch_size, threads)
TEST_CASES = [
    (MOSAICDB, 'TPC-C', 8, 64),
    (OLTPIM, 'TPC-C', 64, 64),
    (MOSAICDB, 'TPC-C1', 8, 64),
    (OLTPIM, 'TPC-C1', 64, 64),
    (MOSAICDB, 'TPC-C2', 8, 64),
    (OLTPIM, 'TPC-C2', 64, 64),
    (MOSAICDB, 'TPC-C3', 8, 64),
    (OLTPIM, 'TPC-C3', 64, 64),
]

BENCH_SECONDS = 30
HUGETLB_SIZE_GB = 180

if __name__ == "__main__":
    args = parse_args()
    create_result_file(args, EXP_NAME)
    print_header(args)
    for system, workload, corobatch_size, threads in TEST_CASES:
         run(args, system, workload, WORKLOAD_SIZE,
             BENCH_SECONDS, threads, HUGETLB_SIZE_GB,
             coro_batch_size=corobatch_size)
