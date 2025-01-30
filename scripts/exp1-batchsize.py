from common import *
import csv
import matplotlib.lines as mline
import matplotlib.patches as mpatch
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

EXP_NAME = 'batchsize'
SYSTEMS = ['MosaicDB', 'OLTPim']
MULTIGET_OPTION = {
    'MosaicDB': [True],
    'OLTPim': [False, True]
}
WORKLOADS = ['YCSB-B']
WORKLOAD_SIZE = 10 ** 8
CORO_BATCH_SIZES = [
    2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
]
BENCH_SECONDS = 60
HUGETLB_SIZE_GB = 180

def plot(args):
    batchsize = {MOSAICDB: [], OLTPIM: []}
    tput = {MOSAICDB: [], OLTPIM: []}
    p99 = {MOSAICDB: [], OLTPIM: []}
    dramRd = {MOSAICDB: [], OLTPIM: []}
    dramWr = {MOSAICDB: [], OLTPIM: []}
    pimRd = {MOSAICDB: [], OLTPIM: []}
    pimWr = {MOSAICDB: [], OLTPIM: []}
    for system in SYSTEMS:
        with open(result_file_path(args, EXP_NAME, system), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                system = row['system']
                batchsize[system].append(float(row['corobatchsize']))
                tput[system].append(float(row['commits']) / float(row['time(s)']) / 1000000)
                p99[system].append(float(row['p99(ms)']))
                dramRd[system].append(float(row['dram.rd(MiB)']) / float(row['commits']) * (1024*1024*1024/1000000))
                dramWr[system].append(float(row['dram.wr(MiB)']) / float(row['commits']) * (1024*1024*1024/1000000))
                pimRd[system].append(float(row['pim.rd(MiB)']) / float(row['commits']) * (1024*1024*1024/1000000))
                pimWr[system].append(float(row['pim.wr(MiB)']) / float(row['commits']) * (1024*1024*1024/1000000))

    fig, axes = plt.subplots(2, 2, figsize=(5.5, 4), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    axes[0][0].plot(batchsize[MOSAICDB], tput[MOSAICDB], linestyle='-', marker='o', color='black', label=MOSAICDB)
    axes[0][0].plot(batchsize[OLTPIM], tput[OLTPIM], linestyle='-', marker='o', color='red', label=OLTPIM)
    axes[0][0].set_xlabel('')
    axes[0][0].set_ylabel('Throughput (MTPS)')
    axes[0][0].set_xscale('log')
    axes[0][0].xaxis.set_major_formatter(formatter)
    axes[0][0].yaxis.set_major_formatter(formatter)
    axes[0][0].set_xlim(1, 2000)
    axes[0][0].set_ylim(bottom=0)
    axes[0][0].minorticks_off()
    axes[0][1].plot(batchsize[MOSAICDB], p99[MOSAICDB], linestyle='-', marker='o', color='black', label=MOSAICDB)
    axes[0][1].plot(batchsize[OLTPIM], p99[OLTPIM], linestyle='-', marker='o', color='red', label=OLTPIM)
    axes[0][1].set_xlabel('')
    axes[0][1].set_ylabel('P99 Latency (ms)')
    axes[0][1].set_xscale('log')
    axes[0][1].set_yscale('log')
    axes[0][1].xaxis.set_major_formatter(formatter)
    axes[0][1].yaxis.set_major_formatter(formatter)
    axes[0][1].set_xlim(1, 2000)
    axes[0][1].minorticks_off()
    bw = {
        MOSAICDB: [r + w for r, w in zip(dramRd[MOSAICDB], dramWr[MOSAICDB])],
        OLTPIM: [dr+ dw + pr + pw for dr, dw, pr, pw in zip(dramRd[OLTPIM], dramWr[OLTPIM], pimRd[OLTPIM], pimWr[OLTPIM])]
    }
    axes[1][0].plot(batchsize[MOSAICDB], bw[MOSAICDB], linestyle='-', marker='o', color='black', label=MOSAICDB)
    axes[1][0].plot(batchsize[OLTPIM], bw[OLTPIM], linestyle='-', marker='o', color='red', label=OLTPIM)
    axes[1][0].set_xlabel('')
    axes[1][0].set_ylabel('Memory Traffic (KB/txn)')
    axes[1][0].set_xscale('log')
    axes[1][0].xaxis.set_major_formatter(formatter)
    axes[1][0].yaxis.set_major_formatter(formatter)
    axes[1][0].set_xlim(1, 2000)
    axes[1][0].minorticks_off()
    axes[1][1].axis('off')
    legends = [
        mline.Line2D([0], [0], linestyle='-', marker='o', color='black', label=MOSAICDB),
        mline.Line2D([0], [0], linestyle='-', marker='o', color='red', label=OLTPIM)
    ]
    axes[1][1].legend(handles=legends, loc='center')
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
                for multiget in MULTIGET_OPTION[system]:
                    for coro_batch_size in CORO_BATCH_SIZES:
                        run(args, system, workload, workload_size,
                            BENCH_SECONDS, HUGETLB_SIZE_GB,
                            coro_batch_size=coro_batch_size,
                            no_pim_multiget=(not multiget))
    else:
        plot(args)
