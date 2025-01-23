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

X_LABELS = ['Baseline', 'Offload\nIndex', '+Offload\nVersions', '+Direct\nPIM Access', '+PIM CPU\nInterleave']

def plot(args):
    tputs = {False: [], True: []}
    with open(result_file_path(args, EXP_NAME, MOSAICDB), 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            numa_local = row['NUMALocal'] == 'True'
            tputs[numa_local] += [float(row['tput(TPS)']) / 1000000]
    with open(result_file_path(args, EXP_NAME, OLTPIM), 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            numa_local = row['NUMALocal'] == 'True'
            tputs[numa_local] += [float(row['tput(TPS)']) / 1000000]

    fig, ax = plt.subplots(figsize=(5.5, 2), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(tputs[False]))
    x_labels = X_LABELS
    suf = lambda i: f'{(tputs[i]/tputs[i-1]):.2f}x'
    width = 0.4
    no_numa_indices = [x - width/2 for x in x_indices]
    no_numa_speedups = ['' if i<=1 else f'{(tputs[False][i]/tputs[False][i-1]):.2f}x' for i in range(len(tputs[False]))]
    numa_indices = [x + width/2 for x in x_indices]
    numa_speedups = ['' if i<=1 else f'{(tputs[True][i]/tputs[True][i-1]):.2f}x' for i in range(len(tputs[True]))]
    rects1 = ax.bar(no_numa_indices, tputs[False], width, color='white', edgecolor='green', hatch='xx', label='System-wide')
    ax.bar_label(rects1, labels=no_numa_speedups, padding=3)
    rects2 = ax.bar(numa_indices, tputs[True], width, color='white', edgecolor='blue', hatch='oo', label='NUMA-local Workload')
    ax.bar_label(rects2, labels=numa_speedups, padding=3)
    ax.set_xticks(x_indices)
    ax.set_xticklabels(x_labels)
    ax.set_xlabel('')
    ax.set_ylabel('Throughput (MTPS)')
    ax.set_ymargin(0.2)
    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_formatter(formatter)
    ax.minorticks_off()
    ax.set_xlim(-0.5, len(x_indices) - 0.5)
    ax.legend(loc='upper left', ncol=2)
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
                        no_numa_local_workload=no_numa_local,
                        no_interleave=no_interleave,
                        executable_suffix=suffix)
    else:
        plot(args)
