from common import *
import csv
import matplotlib.patches as mpatch
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

EXP_NAME = 'ycsb'
SYSTEMS = ['MosaicDB', 'OLTPim']
WORKLOADS = [
    'YCSB-C', 'YCSB-B', 'YCSB-A',
    'YCSB-I1', 'YCSB-I2', 'YCSB-I3', 'YCSB-I4',
]
WORKLOAD_SIZES = {
    'YCSB-C': [10 ** 6, 10 ** 7, 10 ** 8, 10 ** 9],
    'YCSB-B': [10 ** 6, 10 ** 7, 10 ** 8, 10 ** 9],
    'YCSB-A': [10 ** 6, 10 ** 7, 10 ** 8, 10 ** 9],
    'YCSB-I1': [10 ** 8],
    'YCSB-I2': [10 ** 8],
    'YCSB-I3': [10 ** 8],
    'YCSB-I4': [10 ** 8],
}
GC_OPTS = {
    'YCSB-C': [True],
    'YCSB-B': [True],
    'YCSB-A': [False, True],
    'YCSB-I1': [True],
    'YCSB-I2': [True],
    'YCSB-I3': [True],
    'YCSB-I4': [True],
}
BENCH_SECONDS = lambda workload, size: \
    30 if (workload == 'YCSB-A' and size == 10**9) else 60
HUGETLB_SIZE_GB = 180

SIZE_TO_LABEL = {10**6: '1M', 10**7: '10M', 10**8: '100M', 10**9: '1B'}
INS_TO_LABEL = {0.25: '25%', 0.5: '50%', 0.75: '75%', 1.0: '100%'}

def plot(args):
    ycsbc = {
        'size': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []}
    }
    ycsbb = {
        'size': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []}
    }
    ycsba = {
        'size': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []}
    }
    ycsba_nogc = {
        'size': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []}
    }
    ycsbi = {
        'ins-ratio': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []}
    }
    def append_to_stats(workload_stats, row):
        
        workload_stats['tput'][system].append(float(row['commits']) / float(row['time(s)']) / 1000000)
        workload_stats['p99'][system].append(float(row['p99(ms)']))
        workload_stats['dramrd'][system].append(float(row['dram.rd(MiB)']) / float(row['commits']) * (1024*1024*1024/1000000))
        workload_stats['dramwr'][system].append(float(row['dram.wr(MiB)']) / float(row['commits']) * (1024*1024*1024/1000000))
        workload_stats['pimrd'][system].append(float(row['pim.rd(MiB)']) / float(row['commits']) * (1024*1024*1024/1000000))
        workload_stats['pimwr'][system].append(float(row['pim.wr(MiB)']) / float(row['commits']) * (1024*1024*1024/1000000))
    for system in SYSTEMS:
        with open(result_file_path(args, EXP_NAME, system), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                system = row['system']
                workload = row['workload']
                if workload == 'YCSB-C':
                    ycsbc['size'][system].append(int(row['workload_size']))
                    append_to_stats(ycsbc, row)
                elif workload == 'YCSB-B':
                    ycsbb['size'][system].append(int(row['workload_size']))
                    append_to_stats(ycsbb, row)
                elif workload == 'YCSB-A':
                    if row['GC'] == 'True':
                        ycsba['size'][system].append(int(row['workload_size']))
                        append_to_stats(ycsba, row)
                    else:
                        ycsba_nogc['size'][system].append(int(row['workload_size']))
                        append_to_stats(ycsba_nogc, row)
                elif workload == 'YCSB-I1':
                    ycsbi['ins-ratio'][system].append(0.25)
                    append_to_stats(ycsbi, row)
                elif workload == 'YCSB-I2':
                    ycsbi['ins-ratio'][system].append(0.5)
                    append_to_stats(ycsbi, row)
                elif workload == 'YCSB-I3':
                    ycsbi['ins-ratio'][system].append(0.75)
                    append_to_stats(ycsbi, row)
                elif workload == 'YCSB-I4':
                    ycsbi['ins-ratio'][system].append(1.0)
                    append_to_stats(ycsbi, row)

    # Overall
    fig, axes = plt.subplots(2, 4, figsize=(8, 4), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(ycsbc['size'][MOSAICDB]))
    size_labels = [SIZE_TO_LABEL[ycsbc['size'][MOSAICDB][x]] for x in x_indices]
    ycsbi_labels = [INS_TO_LABEL[ycsbi['ins-ratio'][MOSAICDB][x]] for x in x_indices]

    tput_ylim = max(
        ycsbc['tput'][MOSAICDB] + ycsbc['tput'][OLTPIM] +
        ycsbb['tput'][MOSAICDB] + ycsbb['tput'][OLTPIM] +
        ycsba['tput'][MOSAICDB] + ycsba['tput'][OLTPIM] +
        ycsba_nogc['tput'][MOSAICDB] + ycsba_nogc['tput'][OLTPIM] +
        ycsbi['tput'][MOSAICDB] + ycsbi['tput'][OLTPIM]
    )
    def tput_plot(axis, workload, indices, labels, title):
        axis.plot(indices, workload['tput'][MOSAICDB], color='black', linestyle='-', marker='o', label=MOSAICDB)
        axis.plot(indices, workload['tput'][OLTPIM], color='red', linestyle='-', marker='o', label=OLTPIM)
        axis.set_xticks(indices)
        axis.set_xticklabels('')
        axis.set_xlabel('')
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_ylim(bottom=0, top=tput_ylim * 1.1)
        axis.title.set_text(title)
    tput_plot(axes[0][0], ycsbc, x_indices, size_labels, 'YCSB-C')
    tput_plot(axes[0][1], ycsbb, x_indices, size_labels, 'YCSB-B')
    tput_plot(axes[0][2], ycsba, x_indices, size_labels, 'YCSB-A')
    axes[0][2].plot(x_indices, ycsba_nogc['tput'][MOSAICDB], color='grey', linestyle='--', marker='*', label=MOSAICDB + ' (no GC)')
    axes[0][2].plot(x_indices, ycsba_nogc['tput'][OLTPIM], color='pink', linestyle='--', marker='*', label=OLTPIM + ' (no GC)')
    tput_plot(axes[0][3], ycsbi, x_indices, ycsbi_labels, 'YCSB-I')
    axes[0][0].set_ylabel('Throughput (MTPS)')
    axes[0][1].set_yticklabels('')
    axes[0][2].set_yticklabels('')
    axes[0][3].set_yticklabels('')

    dram_ylim = max(
        [dr + dw for dr, dw in zip(ycsbc['dramrd'][MOSAICDB], ycsbc['dramwr'][MOSAICDB])] +
        [dr + dw for dr, dw in zip(ycsbb['dramrd'][MOSAICDB], ycsbb['dramwr'][MOSAICDB])] +
        [dr + dw for dr, dw in zip(ycsba['dramrd'][MOSAICDB], ycsba['dramwr'][MOSAICDB])] +
        [dr + dw for dr, dw in zip(ycsbi['dramrd'][MOSAICDB], ycsbi['dramwr'][MOSAICDB])] +
        [dr + dw + pr + pw for dr, dw, pr, pw in zip(ycsbc['dramrd'][OLTPIM], ycsbc['dramwr'][OLTPIM], ycsbc['pimrd'][OLTPIM], ycsbc['pimwr'][OLTPIM])] +
        [dr + dw + pr + pw for dr, dw, pr, pw in zip(ycsbb['dramrd'][OLTPIM], ycsbb['dramwr'][OLTPIM], ycsbb['pimrd'][OLTPIM], ycsbb['pimwr'][OLTPIM])] +
        [dr + dw + pr + pw for dr, dw, pr, pw in zip(ycsba['dramrd'][OLTPIM], ycsba['dramwr'][OLTPIM], ycsba['pimrd'][OLTPIM], ycsba['pimwr'][OLTPIM])] +
        [dr + dw + pr + pw for dr, dw, pr, pw in zip(ycsbi['dramrd'][OLTPIM], ycsbi['dramwr'][OLTPIM], ycsbi['pimrd'][OLTPIM], ycsbi['pimwr'][OLTPIM])]
    )
    def dram_plot(axis, workload, indices, labels):
        width = 0.3
        indices1 = [x - width/2 for x in indices]
        indices2 = [x + width/2 for x in indices]
        bottom = [0 for _ in indices]
        axis.bar(indices1, workload['dramrd'][MOSAICDB], width, bottom=bottom, color='lightgreen', edgecolor='black', hatch='\\\\')
        bottom = [b + n for b, n in zip(bottom, workload['dramrd'][MOSAICDB])]
        axis.bar(indices1, workload['dramwr'][MOSAICDB], width, bottom=bottom, color='green', edgecolor='black', hatch='\\\\')
        bottom = [0 for _ in indices]
        axis.bar(indices2, workload['dramrd'][OLTPIM], width, bottom=bottom, color='lightgreen', edgecolor='red', hatch='//')
        bottom = [b + n for b, n in zip(bottom, workload['dramrd'][OLTPIM])]
        axis.bar(indices2, workload['dramwr'][OLTPIM], width, bottom=bottom, color='green', edgecolor='red', hatch='//')
        bottom = [b + n for b, n in zip(bottom, workload['dramwr'][OLTPIM])]
        axis.bar(indices2, workload['pimrd'][OLTPIM], width, bottom=bottom, color='lightgrey', edgecolor='red', hatch='//')
        bottom = [b + n for b, n in zip(bottom, workload['pimrd'][OLTPIM])]
        axis.bar(indices2, workload['pimwr'][OLTPIM], width, bottom=bottom, color='grey', edgecolor='red', hatch='//')
        axis.set_xticks(indices)
        axis.set_xticklabels(labels)
        axis.set_xlabel('')
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_ylim(bottom=0, top=dram_ylim * 1.1)
    dram_plot(axes[1][0], ycsbc, x_indices, size_labels)
    dram_plot(axes[1][1], ycsbb, x_indices, size_labels)
    dram_plot(axes[1][2], ycsba, x_indices, size_labels)
    dram_plot(axes[1][3], ycsbi, x_indices, ycsbi_labels)
    axes[1][0].set_ylabel('Mem Traffic (KBPT)')
    axes[1][1].set_xlabel('Table Size')
    axes[1][3].set_xlabel('Insert Ratio')
    axes[0][1].set_yticklabels('')
    axes[0][2].set_yticklabels('')
    axes[0][3].set_yticklabels('')

    legh1, legl1 = axes[0][2].get_legend_handles_labels()
    legh2 = [
        mpatch.Patch(facecolor='white', edgecolor='black', hatch='\\\\'),
        mpatch.Patch(facecolor='white', edgecolor='red', hatch='//'), 
        mpatch.Patch(color='lightgreen'), mpatch.Patch(color='green'),
        mpatch.Patch(color='lightgrey'), mpatch.Patch(color='grey'),
    ]
    legl2 = [
        MOSAICDB, OLTPIM, 'DRAM.Rd', 'DRAM.Wr', 'PIM.Rd', 'PIM.Wr',
    ]
    fig.legend(legh1 + legh2, legl1 + legl2, ncol=5, loc='upper center', bbox_to_anchor=(0.5, 0))
    plt.savefig(result_plot_path(args, EXP_NAME, '-overall'), bbox_inches='tight')
    plt.close(fig)

if __name__ == "__main__":
    args = parse_args()
    if not args.plot:
        create_result_file(args, EXP_NAME)
        print_header(args)
        for workload in WORKLOADS:
            for workload_size in WORKLOAD_SIZES[workload]:
                for gc in GC_OPTS[workload]:
                    for system in SYSTEMS:
                        run(args, system, workload, workload_size,
                            BENCH_SECONDS(workload, workload_size), HUGETLB_SIZE_GB,
                            no_gc=(not gc))
    else:
        plot(args)
