from common import *
import csv
import matplotlib.patches as mpatch
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogFormatterSciNotation

SYSTEMS = [MOSAICDB, OLTPIM]

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--result-dir', type=str, required=True,
        help='Path of directory where results are stored and plots will be stored.')
    return parser.parse_args()

def plot_overall(args):
    EXP_NAME_1 = 'ycsb'
    EXP_NAME_2 = 'tpcc'
    PLOT_NAME = 'overall'
    SIZE_TO_LABEL = {10**6: '1M', 10**7: '10M', 10**8: '100M', 10**9: '1B'}
    INS_TO_LABEL = {0.25: '25%', 0.5: '50%', 0.75: '75%', 1.0: '100%'}
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
    ycsbb_nogc = {
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
    tpcc = {
        'threads': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []}
    }
    tpcc_nogc = {
        'threads': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []}
    }
    def append_to_stats(workload_stats, system, row):
        workload_stats['tput'][system].append(float(row['commits']) / float(row['time(s)']) / 1000000)
        workload_stats['p99'][system].append(float(row['p99(ms)']))
        workload_stats['dramrd'][system].append(float(row['dram.rd(MiB)']) / float(row['commits']) * (1024*1024*1024/1000000))
        workload_stats['dramwr'][system].append(float(row['dram.wr(MiB)']) / float(row['commits']) * (1024*1024*1024/1000000))
        workload_stats['pimrd'][system].append(float(row['pim.rd(MiB)']) / float(row['commits']) * (1024*1024*1024/1000000))
        workload_stats['pimwr'][system].append(float(row['pim.wr(MiB)']) / float(row['commits']) * (1024*1024*1024/1000000))
    for system in SYSTEMS:
        with open(result_file_path(args, EXP_NAME_1, system), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                system = row['system']
                workload = row['workload']
                gc = (row['GC'] == 'True')
                if workload == 'YCSB-C':
                    ycsbc['size'][system].append(int(row['workload_size']))
                    append_to_stats(ycsbc, system, row)
                elif workload == 'YCSB-B':
                    if gc:
                        ycsbb['size'][system].append(int(row['workload_size']))
                        append_to_stats(ycsbb, system, row)
                    else:
                        ycsbb_nogc['size'][system].append(int(row['workload_size']))
                        append_to_stats(ycsbb_nogc, system, row)
                elif workload == 'YCSB-A':
                    if gc:
                        ycsba['size'][system].append(int(row['workload_size']))
                        append_to_stats(ycsba, system, row)
                    else:
                        ycsba_nogc['size'][system].append(int(row['workload_size']))
                        append_to_stats(ycsba_nogc, system, row)
                elif workload == 'YCSB-I1':
                    ycsbi['ins-ratio'][system].append(0.25)
                    append_to_stats(ycsbi, system, row)
                elif workload == 'YCSB-I2':
                    ycsbi['ins-ratio'][system].append(0.5)
                    append_to_stats(ycsbi, system, row)
                elif workload == 'YCSB-I3':
                    ycsbi['ins-ratio'][system].append(0.75)
                    append_to_stats(ycsbi, system, row)
                elif workload == 'YCSB-I4':
                    ycsbi['ins-ratio'][system].append(1.0)
                    append_to_stats(ycsbi, system, row)
    for system in SYSTEMS:
        with open(result_file_path(args, EXP_NAME_2, system), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                system = row['system']
                workload = row['workload']
                if workload == 'TPC-C':
                    if row['GC'] == 'True':
                        tpcc['threads'][system].append(int(row['threads']))
                        append_to_stats(tpcc, system, row)
                    else:
                        tpcc_nogc['threads'][system].append(int(row['threads']))
                        append_to_stats(tpcc_nogc, system, row)

    # Overall
    fig, axes = plt.subplots(2, 5, figsize=(10, 4), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(ycsbc['size'][MOSAICDB]))
    size_labels = [SIZE_TO_LABEL[ycsbc['size'][MOSAICDB][x]] for x in x_indices]
    ycsbi_labels = [INS_TO_LABEL[ycsbi['ins-ratio'][MOSAICDB][x]] for x in x_indices]
    tpcc_labels = [str(tpcc['threads'][MOSAICDB][x]) for x in x_indices]

    tput_ylim = max(
        ycsbc['tput'][MOSAICDB] + ycsbc['tput'][OLTPIM] +
        ycsbb['tput'][MOSAICDB] + ycsbb['tput'][OLTPIM] +
        ycsba['tput'][MOSAICDB] + ycsba['tput'][OLTPIM] +
        ycsba_nogc['tput'][MOSAICDB] + ycsba_nogc['tput'][OLTPIM] +
        ycsbi['tput'][MOSAICDB] + ycsbi['tput'][OLTPIM]
    )
    def tput_plot(axis, workload, indices, labels, title, workload_nogc=None, top_ylim=True):
        axis.plot(indices, workload['tput'][MOSAICDB], color='black', linestyle='-', marker='o', label=MOSAICDB)
        axis.plot(indices, workload['tput'][OLTPIM], color='red', linestyle='-', marker='o', label=OLTPIM)
        if workload_nogc:
            axis.plot(x_indices, workload_nogc['tput'][MOSAICDB], color='grey', linestyle='--', marker='*', label=MOSAICDB + ' (no GC)')
            axis.plot(x_indices, workload_nogc['tput'][OLTPIM], color='pink', linestyle='--', marker='*', label=OLTPIM + ' (no GC)')
        axis.set_xticks(indices)
        axis.set_xticklabels('')
        axis.set_xlabel('')
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_xlim(-0.5, len(indices) - 0.5)
        if top_ylim: axis.set_ylim(bottom=0, top=tput_ylim * 1.1)
        else: axis.set_ylim(bottom=0)
        axis.title.set_text(title)
    tput_plot(axes[0][0], ycsbc, x_indices, size_labels, 'YCSB-C')
    tput_plot(axes[0][1], ycsbb, x_indices, size_labels, 'YCSB-B', ycsbb_nogc)
    tput_plot(axes[0][2], ycsba, x_indices, size_labels, 'YCSB-A', ycsba_nogc)
    tput_plot(axes[0][3], ycsbi, x_indices, ycsbi_labels, 'YCSB-I')
    tput_plot(axes[0][4], tpcc, x_indices, tpcc_labels, 'TPC-C', tpcc_nogc, top_ylim=False)
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
    def dram_plot(axis, workload, indices, labels, top_ylim=True):
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
        axis.set_xlim(-0.5, len(indices) - 0.5)
        if top_ylim: axis.set_ylim(bottom=0, top=dram_ylim * 1.1)
        else: axis.set_ylim(bottom=0)
    dram_plot(axes[1][0], ycsbc, x_indices, size_labels)
    dram_plot(axes[1][1], ycsbb, x_indices, size_labels)
    dram_plot(axes[1][2], ycsba, x_indices, size_labels)
    dram_plot(axes[1][3], ycsbi, x_indices, ycsbi_labels)
    dram_plot(axes[1][4], tpcc, x_indices, tpcc_labels, top_ylim=False)
    axes[1][0].set_ylabel('Memory Traffic\nPer Txn (KBPT)')
    axes[1][1].set_xlabel('Table Size')
    axes[1][3].set_xlabel('Insert Ratio')
    axes[1][4].set_xlabel('Threads')
    axes[1][1].set_yticklabels('')
    axes[1][2].set_yticklabels('')
    axes[1][3].set_yticklabels('')

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
    plt.savefig(result_plot_path(args, PLOT_NAME), bbox_inches='tight')
    plt.close(fig)

def plot_batchsize(args):
    EXP_NAME = 'batchsize'
    PLOT_NAME = 'batchsize'
    OLTPIM_NOMULTIGET = OLTPIM + ' (no MultiGet)'
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
    plt.savefig(result_plot_path(args, PLOT_NAME), bbox_inches='tight')
    plt.close(fig)

def plot_breakdown(args):
    EXP_NAME = 'breakdown'
    PLOT_NAME = 'breakdown'
    OLTPIM_IDXONLY = OLTPIM + ' (indexonly)'
    X_LABELS = [
        MOSAICDB,
        '+NUMA\nLocal',
        'Naive\nOLTPim',
        '+Direct\nPIM Access', 
        '+PIM&CPU\nInterleave',
        '+NUMA\nLocal'
    ]
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
    plt.savefig(result_plot_path(args, PLOT_NAME))

if __name__ == "__main__":
    args = parse_args()
    plot_overall(args)
    plot_batchsize(args)
    plot_breakdown(args)
