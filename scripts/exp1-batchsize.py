from common import *

EXP_NAME = 'batchsize'
SYSTEMS = [MOSAICDB, OLTPIM]
MULTIGET_OPTION = {
    MOSAICDB: [True],
    OLTPIM: [False, True]
}
WORKLOADS = ['YCSB-B']
WORKLOAD_SIZE = 10 ** 8
CORO_BATCH_SIZES = [
    2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
]
BENCH_SECONDS = 60
BENCH_THREADS = 64
HUGETLB_SIZE_GB = 180

if __name__ == "__main__":
    args = parse_args()
    create_result_file(args, EXP_NAME)
    print_header(args)
    for workload in WORKLOADS:
        workload_size = WORKLOAD_SIZE
        for system in SYSTEMS:
            for multiget in MULTIGET_OPTION[system]:
                for coro_batch_size in CORO_BATCH_SIZES:
                    run(args, system, workload, workload_size,
                        BENCH_SECONDS, BENCH_THREADS, HUGETLB_SIZE_GB,
                        coro_batch_size=coro_batch_size,
                        no_pim_multiget=(not multiget))
