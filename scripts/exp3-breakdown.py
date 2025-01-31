from common import *
import csv
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

EXP_NAME = 'breakdown'
WORKLOADS = ['YCSB-B']
WORKLOAD_SIZES = [10 ** 8]
# (system, no_numa_local, no_interleave, suffix)
#BREAKDOWNS = [
#    ('MosaicDB', True, False, None),
#    ('OLTPim', True, True, '_indexonly_nodirect'),
#    ('OLTPim', True, True, '_nodirect'),
#    ('OLTPim', True, True, None),
#    ('OLTPim', True, False, None),
#    ('MosaicDB', False, False, None),
#    ('OLTPim', False, True, '_indexonly_nodirect'),
#    ('OLTPim', False, True, '_nodirect'),
#    ('OLTPim', False, True, None),
#    ('OLTPim', False, False, None),
#]
BREAKDOWNS = [
    (MOSAICDB, True, False, None),
    (MOSAICDB, False, False, None),
    (OLTPIM, True, True, '_indexonly_nodirect'),
    (OLTPIM, True, True, '_indexonly'),
    (OLTPIM, True, False, '_indexonly'),
    (OLTPIM, False, False, '_indexonly'),
    (OLTPIM, True, True, '_nodirect'),
    (OLTPIM, True, True, None),
    (OLTPIM, True, False, None),
    (OLTPIM, False, False, None),
]
BENCH_SECONDS = 60
HUGETLB_SIZE_GB = 180
OLTPIM_IDXONLY = OLTPIM + ' (indexonly)'

X_LABELS = [
    'MosaicDB', 
    '+NUMA\nLocal',
    'Naive\nOLTPim',
    '+Direct\nPIM Access', 
    '+PIM&CPU\nInterleave',
    '+NUMA\nLocal'
]

def plot(args):
    stats = {
        'tput': {MOSAICDB: [], OLTPIM_IDXONLY: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM_IDXONLY: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM_IDXONLY: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM_IDXONLY: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM_IDXONLY: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM_IDXONLY: [], OLTPIM: []},
        'totalbw': {MOSAICDB: [], OLTPIM_IDXONLY: [], OLTPIM: []}
    }
    def append_to_stats(system, row):
        stats['tput'][system].append(float(row['commits']) / float(row['time(s)']) / 1000000)
        stats['p99'][system].append(float(row['p99(ms)']))
        per_txn_bw = lambda name: float(row[name]) / float(row['commits']) * (1024*1024*1024/1000000)
        dramrd = per_txn_bw('dram.rd(MiB)')
        dramwr = per_txn_bw('dram.wr(MiB)')
        pimrd = per_txn_bw('pim.rd(MiB)')
        pimwr = per_txn_bw('pim.wr(MiB)')
        stats['dramrd'][system].append(dramrd)
        stats['dramwr'][system].append(dramwr)
        stats['pimrd'][system].append(pimrd)
        stats['pimwr'][system].append(pimwr)
        stats['totalbw'][system].append(dramrd + dramwr + pimrd + pimwr)
    with open(result_file_path(args, EXP_NAME, MOSAICDB), 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            append_to_stats(MOSAICDB, row)
    with open(result_file_path(args, EXP_NAME, OLTPIM), 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if 'indexonly' in row['suffix']:
                append_to_stats(OLTPIM_IDXONLY, row)
            else:
                append_to_stats(OLTPIM, row)

    fig, axes = plt.subplots(2, 1, figsize=(6, 4), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(X_LABELS))
    x_labels = X_LABELS

    def draw_bar(axis, ydata0, ydata1, ydata2, ylabel, datalabels):
        width = 0.4
        labelgen = lambda ys: [f"{y:.1f}" for y in ys]
        num_y0 = len(ydata0)
        indices0 = range(num_y0)
        indices1 = [x - width/2 for x in range(num_y0, len(X_LABELS))]
        indices2 = [x + width/2 for x in range(num_y0, len(X_LABELS))]
        rects0 = axis.bar(indices0, ydata0, 1.5*width, color='white', edgecolor='black', hatch='\\\\', label=MOSAICDB)
        rects1 = axis.bar(indices1, ydata1, width, color='white', edgecolor='pink', hatch='/', label='PIM Index')
        rects2 = axis.bar(indices2, ydata2, width, color='white', edgecolor='red', hatch='//', label='PIM Index&Versions')
        if datalabels:
            axis.bar_label(rects0, labels=labelgen(ydata0), padding=3)
            axis.bar_label(rects1, labels=labelgen(ydata1), padding=3)
            axis.bar_label(rects2, labels=labelgen(ydata2), padding=3)
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
        axis.axvline(num_y0 - 0.5, color='black', linestyle='--', linewidth=1)
    draw_bar(axes[0], stats['tput'][MOSAICDB], stats['tput'][OLTPIM_IDXONLY], stats['tput'][OLTPIM], 'Throughput (MTPS)', True)
    draw_bar(axes[1], stats['totalbw'][MOSAICDB], stats['totalbw'][OLTPIM_IDXONLY], stats['totalbw'][OLTPIM], 'Memory Traffic\nPer Txn (KBPT)', False)
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
