from common import *
import csv
import matplotlib.lines as mline
import matplotlib.patches as mpatch
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogFormatterSciNotation

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
BENCH_THREADS = 64
HUGETLB_SIZE_GB = 180
OLTPIM_NOMULTIGET = OLTPIM + ' (no MultiGet)'

def plot(args):
    stats = {
        'batchsize': {MOSAICDB: [], OLTPIM_NOMULTIGET: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM_NOMULTIGET: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM_NOMULTIGET: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM_NOMULTIGET: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM_NOMULTIGET: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM_NOMULTIGET: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM_NOMULTIGET: [], OLTPIM: []},
        'totalbw': {MOSAICDB: [], OLTPIM_NOMULTIGET: [], OLTPIM: []},
        'abort': {MOSAICDB: [], OLTPIM_NOMULTIGET: [], OLTPIM: []},
    }
    def append_to_stats(workload_stats, system, row):
        workload_stats['batchsize'][system].append(int(row['corobatchsize']))
        workload_stats['tput'][system].append(float(row['commits']) / float(row['time(s)']) / 1000000)
        workload_stats['p99'][system].append(float(row['p99(ms)']))
        per_txn_bw = lambda name: float(row[name]) / float(row['commits']) * (1024*1024*1024/1000000)
        dramrd = per_txn_bw('dram.rd(MiB)')
        dramwr = per_txn_bw('dram.wr(MiB)')
        pimrd = per_txn_bw('pim.rd(MiB)')
        pimwr = per_txn_bw('pim.wr(MiB)')
        workload_stats['dramrd'][system].append(dramrd)
        workload_stats['dramwr'][system].append(dramwr)
        workload_stats['pimrd'][system].append(pimrd)
        workload_stats['pimwr'][system].append(pimwr)
        workload_stats['totalbw'][system].append(dramrd + dramwr + pimrd + pimwr)
        n_commits = float(row['commits'])
        n_aborts = float(row['aborts'])
        workload_stats['abort'][system].append(n_aborts / (n_commits + n_aborts) * 100)
    for system in SYSTEMS:
        with open(result_file_path(args, EXP_NAME, system), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                system = row['system']
                if system == OLTPIM and row['PIMMultiget'] == 'False':
                    system = OLTPIM_NOMULTIGET
                append_to_stats(stats, system, row)

    fig, axes = plt.subplots(2, 2, figsize=(5, 3.5), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')

    def simple_plot(axis, yname, ylabel, ylog):
        axis.plot(stats['batchsize'][MOSAICDB], stats[yname][MOSAICDB], linestyle='-', marker='o', color='black', label=MOSAICDB)
        axis.plot(stats['batchsize'][OLTPIM_NOMULTIGET], stats[yname][OLTPIM_NOMULTIGET], linestyle='--', marker='*', color='pink', label=OLTPIM_NOMULTIGET)
        axis.plot(stats['batchsize'][OLTPIM], stats[yname][OLTPIM], linestyle='-', marker='o', color='red', label=OLTPIM)
        axis.set_xlabel('')
        axis.set_ylabel(ylabel)
        axis.set_xscale('log')
        if ylog: axis.set_yscale('log')
        else: axis.set_ylim(bottom=0)
        axis.set_xticks([1, 10, 100, 1000])
        axis.xaxis.set_major_formatter(formatter)
        axis.yaxis.set_major_formatter(formatter)
        axis.set_xlim(1, 2000)
        axis.minorticks_off()
    simple_plot(axes[0][0], 'tput', 'Throughput (MTPS)', False)
    simple_plot(axes[0][1], 'p99', 'P99 Latency (ms)', True)
    simple_plot(axes[1][0], 'totalbw', 'Memory Traffic\nPer Txn (KBPT)', False)
    simple_plot(axes[1][1], 'abort', 'Abort Rate (%)', True)
    axes[0][0].set_xticklabels('')
    axes[0][1].set_xticklabels('')
    bw_max = max(stats['totalbw'][MOSAICDB] + stats['totalbw'][OLTPIM])
    axes[1][0].set_ylim(top=bw_max * 1.2)
    axes[1][1].yaxis.set_major_formatter(LogFormatterSciNotation())
    legh, legl = axes[0][0].get_legend_handles_labels()
    fig.supxlabel('Coroutine Batchsize per Thread')
    fig.legend(legh, legl, ncol=3, loc='upper center', bbox_to_anchor=(0.5, 0))
    plt.savefig(result_plot_path(args, EXP_NAME), bbox_inches='tight')
    plt.close(fig)

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
                            BENCH_SECONDS, BENCH_THREADS, HUGETLB_SIZE_GB,
                            coro_batch_size=coro_batch_size,
                            no_pim_multiget=(not multiget))
    else:
        plot(args)
