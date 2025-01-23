from common import *
import csv
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

EXP_NAME = 'batchsize'
SYSTEMS = ['MosaicDB', 'OLTPim']
WORKLOADS = ['YCSB-C']
WORKLOAD_SIZE = 10 ** 8
CORO_BATCH_SIZES = [
    2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
]

def plot(args):
    batchsize = {MOSAICDB: [], OLTPIM: []}
    tput = {MOSAICDB: [], OLTPIM: []}
    p99 = {MOSAICDB: [], OLTPIM: []}
    for system in SYSTEMS:
        with open(result_file_path(args, EXP_NAME, system), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                system = row['system']
                batchsize[system] += [float(row['corobatchsize'])]
                tput[system] += [float(row['tput(TPS)']) / 1000000]
                p99[system] += [float(row['p99(ms)'])]

    fig, axes = plt.subplots(1, 2, figsize=(5.5, 2), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    axes[0].plot(batchsize[MOSAICDB], tput[MOSAICDB], linestyle='-', marker='o', color='black', label=MOSAICDB)
    axes[0].plot(batchsize[OLTPIM], tput[OLTPIM], linestyle='-', marker='o', color='red', label=OLTPIM)
    axes[0].set_xlabel('')
    axes[0].set_ylabel('Throughput (MTPS)')
    axes[0].set_xscale('log')
    axes[0].xaxis.set_major_formatter(formatter)
    axes[0].yaxis.set_major_formatter(formatter)
    axes[0].set_xlim(1, 2000)
    axes[0].set_ylim(bottom=0)
    axes[0].minorticks_off()
    axes[1].plot(batchsize[MOSAICDB], p99[MOSAICDB], linestyle='-', marker='o', color='black', label=MOSAICDB)
    axes[1].plot(batchsize[OLTPIM], p99[OLTPIM], linestyle='-', marker='o', color='red', label=OLTPIM)
    axes[1].set_xlabel('')
    axes[1].set_ylabel('P99 Latency (ms)')
    axes[1].set_xscale('log')
    axes[1].set_yscale('log')
    axes[1].xaxis.set_major_formatter(formatter)
    axes[1].yaxis.set_major_formatter(formatter)
    axes[1].set_xlim(1, 2000)
    axes[1].minorticks_off()
    axes[1].legend(loc='upper left')
    fig.supxlabel('Coroutine Batchsize per Thread')
    plt.savefig(result_plot_path(args, EXP_NAME))

if __name__ == "__main__":
    args = parse_args()
    if not args.plot:
        create_result_file(args, EXP_NAME)
        print_header(args)
        for workload in WORKLOADS:
            workload_size = WORKLOAD_SIZE
            for system in SYSTEMS:
                for coro_batch_size in CORO_BATCH_SIZES:
                    run(args, system, workload, workload_size,
                        coro_batch_size)
    else:
        plot(args)
