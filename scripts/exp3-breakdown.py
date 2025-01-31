from common import *
import csv
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

EXP_NAME = 'breakdown'
WORKLOADS = ['YCSB-B']
WORKLOAD_SIZES = [10 ** 8]
# (system, no_numa_local, no_interleave, suffix)
BREAKDOWNS = [
    ('MosaicDB', True, False, None),
    ('OLTPim', True, True, '_indexonly_nodirect'),
    ('OLTPim', True, True, '_nodirect'),
    ('OLTPim', True, True, None),
    ('OLTPim', True, False, None),
    ('MosaicDB', False, False, None),
    ('OLTPim', False, True, '_indexonly_nodirect'),
    ('OLTPim', False, True, '_nodirect'),
    ('OLTPim', False, True, None),
    ('OLTPim', False, False, None),
]
BENCH_SECONDS = 60
HUGETLB_SIZE_GB = 180

X_LABELS = [
    'MosaicDB', 
    'Offload\nIndex', 
    '+Offload\nVersions', 
    '+Direct\nPIM Access', 
    '+PIM CPU\nInterleave'
]

def plot(args):
    sys_wide_stats = {
        'tput': [],
        'p99': [],
        'dramrd': [],
        'dramwr': [],
        'pimrd': [],
        'pimwr': [],
        'totalbw': []
    }
    numa_local_stats = {
        'tput': [],
        'p99': [],
        'dramrd': [],
        'dramwr': [],
        'pimrd': [],
        'pimwr': [],
        'totalbw': []
    }
    def append_to_stats(workload_stats, system, row):
        workload_stats['tput'].append(float(row['commits']) / float(row['time(s)']) / 1000000)
        workload_stats['p99'].append(float(row['p99(ms)']))
        per_txn_bw = lambda name: float(row[name]) / float(row['commits']) * (1024*1024*1024/1000000)
        dramrd = per_txn_bw('dram.rd(MiB)')
        dramwr = per_txn_bw('dram.wr(MiB)')
        pimrd = per_txn_bw('pim.rd(MiB)')
        pimwr = per_txn_bw('pim.wr(MiB)')
        workload_stats['dramrd'].append(dramrd)
        workload_stats['dramwr'].append(dramwr)
        workload_stats['pimrd'].append(pimrd)
        workload_stats['pimwr'].append(pimwr)
        workload_stats['totalbw'].append(dramrd + dramwr + pimrd + pimwr)
    with open(result_file_path(args, EXP_NAME, MOSAICDB), 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['NUMALocal'] == 'True':
                append_to_stats(numa_local_stats, MOSAICDB, row)
            else:
                append_to_stats(sys_wide_stats, MOSAICDB, row)
    with open(result_file_path(args, EXP_NAME, OLTPIM), 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['NUMALocal'] == 'True':
                append_to_stats(numa_local_stats, OLTPIM, row)
            else:
                append_to_stats(sys_wide_stats, OLTPIM, row)

    fig, axes = plt.subplots(2, 1, figsize=(5, 4), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(sys_wide_stats['tput']))
    x_labels = X_LABELS

    def draw_bar(axis, ydata1, ydata2, ylabel, datalabels):
        width = 0.4
        indices1 = [x - width/2 for x in x_indices]
        labels1 = [f"{ydata1[x]:.1f}" for x in x_indices]
        indices2 = [x + width/2 for x in x_indices]
        labels2 = [f"{ydata2[x]:.1f}" for x in x_indices]
        rects1 = axis.bar(indices1, ydata1, width, color='white', edgecolor='green', hatch='xx', label='System-wide')
        rects2 = axis.bar(indices2, ydata2, width, color='white', edgecolor='blue', hatch='oo', label='NUMA-local Workload')
        if datalabels:
            axis.bar_label(rects1, labels=labels1, padding=3)
            axis.bar_label(rects2, labels=labels2, padding=3)
        axis.set_xticks(x_indices)
        axis.set_xticklabels('')
        axis.set_xlabel('')
        axis.set_ylabel(ylabel)
        if datalabels:
            axis.set_ymargin(0.2)
        axis.set_ylim(bottom=0)
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_xlim(-0.5, len(x_indices) - 0.5)
    draw_bar(axes[0], sys_wide_stats['tput'], numa_local_stats['tput'], 'Throughput (MTPS)', True)
    draw_bar(axes[1], sys_wide_stats['totalbw'], numa_local_stats['totalbw'], 'Memory Traffic\nPer Txn (KBPT)', False)
    axes[1].legend(loc='upper right', ncol=1)
    axes[1].set_xticklabels(x_labels)
    plt.savefig(result_plot_path(args, EXP_NAME))

if __name__ == "__main__":
    args = parse_args()
    if not args.plot:
        create_result_file(args, EXP_NAME)
        print_header(args)
        for workload in WORKLOADS:
            for workload_size in WORKLOAD_SIZES:
                for system, no_numa_local, no_interleave, suffix in BREAKDOWNS:
                    run(args, system, workload, workload_size, 
                        BENCH_SECONDS, HUGETLB_SIZE_GB,
                        no_numa_local_workload=no_numa_local,
                        no_interleave=no_interleave,
                        executable_suffix=suffix)
    else:
        plot(args)
