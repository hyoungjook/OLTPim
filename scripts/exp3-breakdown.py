from common import *
import csv
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

EXP_NAME = 'breakdown'
WORKLOADS = ['YCSB-C']
WORKLOAD_SIZES = [10 ** 8]
# (system, no_numa_local, no_interleave, suffix)
BREAKDOWNS = [
    ('MosaicDB', True, False, None),
    ('MosaicDB', False, False, None),
    ('OLTPim', True, True, '_indexonly_nodirect'),
    ('OLTPim', True, True, '_nodirect'),
    ('OLTPim', True, True, None),
    ('OLTPim', True, False, None),
    ('OLTPim', False, False, None),
]

X_LABELS = [
    'Default', 'NUMA Local\nWorkload',
    'Offload\nIndex', 'Offload\nIndex & Ver', 'Direct\nPIM Access', 'CPU-PIM\nInterleave', 'NUMA Local\nWorkload'
]

def plot(args):
    tputs = []
    with open(result_file_path(args, EXP_NAME), 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            tputs += [float(row['tput(TPS)']) / 1000000]

    fig, ax = plt.subplots(figsize=(5, 3), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(tputs))
    x_labels = X_LABELS
    suf = lambda i: f'{(tputs[i]/tputs[i-1]):.2f}x'
    mosaic_indices = [0, 1]
    mosaic_tputs = tputs[0:2]
    mosaic_speedups = ['', suf(1)]
    oltpim_indices = [2, 3, 4, 5, 6]
    oltpim_tputs = tputs[2:7]
    oltpim_speedups = ['', suf(3), suf(4), suf(5), suf(6)]
    width = 0.3
    rects1 = ax.bar(mosaic_indices, mosaic_tputs, width, color='white', edgecolor='black', hatch='\\\\', label='MosaicDB')
    ax.bar_label(rects1, labels=mosaic_speedups, padding=3)
    rects2 = ax.bar(oltpim_indices, oltpim_tputs, width, color='white', edgecolor='red', hatch='//', label='OLTPim')
    ax.bar_label(rects2, labels=oltpim_speedups, padding=3)
    ax.set_xticks(x_indices)
    ax.set_xticklabels(x_labels)
    ax.tick_params(axis='x', which='major', labelsize=7)
    ax.set_xlabel('')
    ax.set_ylabel('Throughput (MTPS)')
    ax.set_ylim(0, 10.5)
    ax.yaxis.set_major_formatter(formatter)
    ax.minorticks_off()
    ax.set_xlim(-0.5, len(x_indices) - 0.5)
    ax.legend(loc='upper left')
    plt.savefig(result_plot_path(args, EXP_NAME))

if __name__ == "__main__":
    args = parse_args()
    if args.measure:
        create_result_file(args, EXP_NAME)
        print_header(args)
        for workload in WORKLOADS:
            for workload_size in WORKLOAD_SIZES:
                for system, no_numa_local, no_interleave, suffix in BREAKDOWNS:
                    run(args, system, workload, workload_size, 
                        no_numa_local_workload=no_numa_local,
                        no_interleave=no_interleave,
                        executable_suffix=suffix)
    plot(args)
