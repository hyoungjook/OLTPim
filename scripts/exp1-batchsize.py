from common import *

EXP_NAME = 'batchsize'
SYSTEMS = ['MosaicDB', 'OLTPim']
WORKLOADS = ['YCSB-C']
WORKLOAD_SIZE = 10 ** 9
CORO_BATCH_SIZES = [
    2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
]

if __name__ == "__main__":
    args = parse_args()
    create_result_file(args, EXP_NAME)
    print_header(args)
    for workload in WORKLOADS:
        workload_size = WORKLOAD_SIZE
        for system in SYSTEMS:
            for coro_batch_size in CORO_BATCH_SIZES:
                run(args, system, workload, workload_size,
                    coro_batch_size)
