from common import *
import csv
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

EXP_NAME = 'tpcc'
SYSTEMS = ['MosaicDB', 'OLTPim']
WORKLOADS = ['TPC-C', 'TPC-CR']
WORKLOAD_SIZES = [1024, 2048, 4096]

def plot(args):
    pass

if __name__ == "__main__":
    args = parse_args()
    if not args.plot:
        create_result_file(args, EXP_NAME)
        print_header(args)
        for workload in WORKLOADS:
            for workload_size in WORKLOAD_SIZES:
                for system in SYSTEMS:
                    run(args, system, workload, workload_size)
    else:
        plot(args)
