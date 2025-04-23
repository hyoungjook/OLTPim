from common import *
import csv
import matplotlib.legend_handler as mlegh
import matplotlib.lines as mline
import matplotlib.patches as mpatch
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogFormatterSciNotation

SYSTEMS = [MOSAICDB, OLTPIM]

COLORS = {
    'mosaicdb': 'black',
    'mosaicdb-light': 'grey',
    'oltpim': '#C41230',
    'oltpim-light': '#E6A1AC',
    'dramrd': '#8AE6AB',
    'dramwr': '#1A9949',
    'pimrd': '#E6B98A',
    'pimwr': '#995C1A',
    'transparent': '#ffffff00',
}

STYLES = {
    'mosaicdb': {
        'plot': {'color': COLORS['mosaicdb'], 'linestyle': '-', 'marker': 'X'},
        'plot-highlight': {'color': COLORS['mosaicdb'], 'marker': '*', 'markersize': 15},
        'bar': {'edgecolor': COLORS['mosaicdb'], 'facecolor': 'white', 'hatch': None},
        'bar-cover': {'edgecolor': COLORS['mosaicdb'], 'facecolor': COLORS['transparent']},
    },
    'oltpim': {
        'plot': {'color': COLORS['oltpim'], 'linestyle': '-', 'marker': 'o'},
        'plot-highlight': {'color': COLORS['oltpim'], 'marker': '*', 'markersize': 15},
        'bar': {'edgecolor': COLORS['oltpim'], 'facecolor': 'white', 'hatch': 'oo'},
        'bar-cover': {'edgecolor': COLORS['oltpim'], 'facecolor': COLORS['transparent']},
    },
    'mosaicdb-light': {
        'plot': {'color': COLORS['mosaicdb-light'], 'linestyle': '--', 'marker': 'P'},
    },
    'oltpim-light': {
        'plot': {'color': COLORS['oltpim-light'], 'linestyle': '--', 'marker': '^'},
        'bar': {'edgecolor': COLORS['oltpim-light'], 'facecolor': 'white', 'hatch': '||'},
        'bar-cover': {'edgecolor': COLORS['oltpim-light'], 'facecolor': COLORS['transparent']},
    },
    'dramrd': {'edgecolor': 'black', 'facecolor': COLORS['dramrd'], 'hatch': '\\\\\\', 'linewidth': 0, 'hatch_linewidth': 0.5},
    'dramwr': {'edgecolor': 'black', 'facecolor': COLORS['dramwr'], 'hatch': '///', 'linewidth': 0, 'hatch_linewidth': 0.5},
    'pimrd': {'edgecolor': 'black', 'facecolor': COLORS['pimrd'], 'hatch': '+++', 'linewidth': 0, 'hatch_linewidth': 0.5},
    'pimwr': {'edgecolor': 'black', 'facecolor': COLORS['pimwr'], 'hatch': 'xxx', 'linewidth': 0, 'hatch_linewidth': 0.5},
}

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--result-dir', type=str, required=True,
        help='Path of directory where results are stored and plots will be stored.')
    return parser.parse_args()

def plot_intro(args):
    EXP_NAME = 'ycsb'
    PLOT_NAME = 'intro'
    LABELS = ['YCSB-C', 'YCSB-B']
    stats_1B = {
        'workload': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'totalbw': {MOSAICDB: [], OLTPIM: []},
    }
    def append_to_stats(system, row):
        stats_1B['workload'][system].append(row['workload'])
        stats_1B['tput'][system].append(float(row['commits']) / float(row['time(s)']) / 1000000)
        per_txn_bw = lambda name: float(row[name]) / float(row['commits']) * (1024*1024*1024/1000000)
        dramrd = per_txn_bw('dram.rd(MiB)')
        dramwr = per_txn_bw('dram.wr(MiB)')
        pimrd = per_txn_bw('pim.rd(MiB)')
        pimwr = per_txn_bw('pim.wr(MiB)')
        stats_1B['totalbw'][system].append(dramrd + dramwr + pimrd + pimwr)
    for system in SYSTEMS:
        with open(result_file_path(args, EXP_NAME, system), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                system = row['system']
                workload = row['workload']
                if int(row['workload_size']) == 10**9 and row['GC'] == 'True' \
                        and float(row['YCSBzipfian']) == 0:
                    if workload in ['YCSB-C', 'YCSB-B']:
                        append_to_stats(system, row)
    assert len(stats_1B['workload'][MOSAICDB]) == len(LABELS) and \
            len(stats_1B['workload'][OLTPIM]) == len(LABELS)

    fig, axes = plt.subplots(1, 2, figsize=(5, 1.8), constrained_layout=True,
        gridspec_kw={'wspace': 0.1})
    formatter = FuncFormatter(lambda x, _: f'{x:g}')

    def simple_bar(axis, yname, ylabel):
        width = 0.3
        indices = [1, 0]
        axis.bar([x - width/2 for x in indices], stats_1B[yname][MOSAICDB], width, label=MOSAICDB, **STYLES['mosaicdb']['bar'])
        axis.bar([x + width/2 for x in indices], stats_1B[yname][OLTPIM], width, label=OLTPIM, **STYLES['oltpim']['bar'])
        speedups = [f'{max(m,o)/min(m,o):.2f}x' for m, o in zip(stats_1B[yname][MOSAICDB], stats_1B[yname][OLTPIM])]
        maxs = [max(m, o) for m, o in zip(stats_1B[yname][MOSAICDB], stats_1B[yname][OLTPIM])]
        rects = axis.bar(indices, maxs, alpha=0)
        axis.bar_label(rects, labels=speedups, padding=3)
        axis.set_xticks(indices)
        axis.set_xticklabels(LABELS)
        axis.set_xlabel('')
        axis.set_ylabel(ylabel)
        axis.set_ymargin(0.2)
        axis.set_ylim(bottom=0)
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_xlim(-0.5, len(indices) - 0.5)
    simple_bar(axes[0], 'tput', 'Throughput\n  (MTPS) $\\bf{→}$')
    simple_bar(axes[1], 'totalbw', 'Memory Traffic\n$\\bf{←}$ (KBPT)  ')
    legh, legl = axes[0].get_legend_handles_labels()
    fig.legend(legh, legl, ncol=2, loc='lower center', bbox_to_anchor=(0.5, 1))
    fig.align_ylabels()
    plt.savefig(result_plot_path(args, PLOT_NAME), bbox_inches='tight')
    plt.close(fig)

def plot_overall(args):
    EXP_NAME_1 = 'ycsb'
    EXP_NAME_2 = 'tpcc'
    PLOT_NAME = 'overall'
    SIZE_TO_LABEL = {10**6: '1M', 10**7: '10M', 10**8: '100M', 10**9: '1B'}
    INS_TO_LABEL = {0.1: '10%', 0.2: '20%', 0.5: '50%', 1.0: '100%'}
    ycsbc = {
        'size': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []},
        'totalbw': {MOSAICDB: [], OLTPIM: []},
    }
    ycsbb = {
        'size': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []},
        'totalbw': {MOSAICDB: [], OLTPIM: []},
    }
    ycsba = {
        'size': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []},
        'totalbw': {MOSAICDB: [], OLTPIM: []},
    }
    ycsbi = {
        'ins-ratio': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []},
        'totalbw': {MOSAICDB: [], OLTPIM: []},
    }
    ycsbs = {
        'scan-len': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []},
        'totalbw': {MOSAICDB: [], OLTPIM: []},
    }
    tpcc = {
        'ratio': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []},
        'totalbw': {MOSAICDB: [], OLTPIM: []},
    }
    def append_to_stats(workload_stats, system, row):
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
    for system in SYSTEMS:
        with open(result_file_path(args, EXP_NAME_1, system), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                system = row['system']
                workload = row['workload']
                logging = (row['log'] == 'True')
                if workload == 'YCSB-C':
                    ycsbc['size'][system].append(int(row['workload_size']))
                    append_to_stats(ycsbc, system, row)
                elif workload == 'YCSB-B':
                    if float(row['YCSBzipfian']) == 0:
                        ycsbb['size'][system].append(int(row['workload_size']))
                        append_to_stats(ycsbb, system, row)
                elif workload == 'YCSB-A':
                    if row['GC'] == 'True':
                        ycsba['size'][system].append(int(row['workload_size']))
                        append_to_stats(ycsba, system, row)
                elif workload == 'YCSB-I1':
                    if logging:
                        ycsbi['ins-ratio'][system].append(0.1)
                        append_to_stats(ycsbi, system, row)
                elif workload == 'YCSB-I2':
                    if logging:
                        ycsbi['ins-ratio'][system].append(0.2)
                        append_to_stats(ycsbi, system, row)
                elif workload == 'YCSB-I3':
                    if logging:
                        ycsbi['ins-ratio'][system].append(0.5)
                        append_to_stats(ycsbi, system, row)
                elif workload == 'YCSB-I4':
                    if logging:
                        ycsbi['ins-ratio'][system].append(1.0)
                        append_to_stats(ycsbi, system, row)
                elif workload == 'YCSB-S2':
                    ycsbs['scan-len'][system].append(2)
                    append_to_stats(ycsbs, system, row)
                elif workload == 'YCSB-S4':
                    ycsbs['scan-len'][system].append(4)
                    append_to_stats(ycsbs, system, row)
                elif workload == 'YCSB-S8':
                    ycsbs['scan-len'][system].append(8)
                    append_to_stats(ycsbs, system, row)
                elif workload == 'YCSB-S16':
                    ycsbs['scan-len'][system].append(16)
                    append_to_stats(ycsbs, system, row)
    for system in SYSTEMS:
        with open(result_file_path(args, EXP_NAME_2, system), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                system = row['system']
                workload = row['workload']
                if workload == 'TPC-C':
                    tpcc['ratio'][system].append('9:1')
                    append_to_stats(tpcc, system, row)
                elif workload == 'TPC-C1':
                    tpcc['ratio'][system].append('6:4')
                    append_to_stats(tpcc, system, row)
                elif workload == 'TPC-C2':
                    tpcc['ratio'][system].append('3:7')
                    append_to_stats(tpcc, system, row)
                elif workload == 'TPC-C3':
                    tpcc['ratio'][system].append('0:10')
                    append_to_stats(tpcc, system, row)

    fig, axes = plt.subplots(2, 6, figsize=(12, 3), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(ycsbc['size'][MOSAICDB]))
    size_labels = [SIZE_TO_LABEL[ycsbc['size'][MOSAICDB][x]] for x in x_indices]
    ycsbi_labels = [INS_TO_LABEL[ycsbi['ins-ratio'][MOSAICDB][x]] for x in x_indices]
    ycsbs_labels = [str(ycsbs['scan-len'][MOSAICDB][x]) for x in x_indices]
    tpcc_labels = [str(tpcc['ratio'][MOSAICDB][x]) for x in x_indices]

    tput_ylim = max(
        ycsbc['tput'][MOSAICDB] + ycsbc['tput'][OLTPIM] +
        ycsbb['tput'][MOSAICDB] + ycsbb['tput'][OLTPIM] +
        ycsba['tput'][MOSAICDB] + ycsba['tput'][OLTPIM] +
        ycsbi['tput'][MOSAICDB] + ycsbi['tput'][OLTPIM] +
        ycsbs['tput'][MOSAICDB] + ycsbs['tput'][OLTPIM]
    )
    def tput_plot(axis, workload, indices, labels, title, top_ylim=True):
        axis.plot(indices, workload['tput'][MOSAICDB], label=MOSAICDB, **STYLES['mosaicdb']['plot'])
        axis.plot(indices, workload['tput'][OLTPIM], label=OLTPIM, **STYLES['oltpim']['plot'])
        axis.set_xticks(indices)
        axis.set_xticklabels('')
        axis.set_xlabel('')
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_xlim(-0.5, len(indices) - 0.5)
        if top_ylim: axis.set_ylim(bottom=0, top=tput_ylim * 1.1)
        else: axis.set_ylim(bottom=0)
        axis.title.set_text(title)
        axis.grid(axis='y', color='lightgrey', linestyle='--')
        axis.set_axisbelow(True)
    tput_plot(axes[0][0], ycsbc, x_indices, size_labels, 'YCSB-C')
    tput_plot(axes[0][1], ycsbb, x_indices, size_labels, 'YCSB-B')
    tput_plot(axes[0][2], ycsba, x_indices, size_labels, 'YCSB-A')
    tput_plot(axes[0][3], ycsbs, x_indices, ycsbs_labels, 'YCSB-S')
    tput_plot(axes[0][4], ycsbi, x_indices, ycsbi_labels, 'YCSB-I')
    tput_plot(axes[0][5], tpcc, x_indices, tpcc_labels, 'TPC-C', top_ylim=False)
    axes[0][0].set_ylabel('Throughput\n  (MTPS) $\\bf{→}$')
    axes[0][1].set_yticklabels('')
    axes[0][2].set_yticklabels('')
    axes[0][3].set_yticklabels('')
    axes[0][4].set_yticklabels('')

    dram_ylim = max(
        ycsbc['totalbw'][MOSAICDB] + ycsbc['totalbw'][OLTPIM] + 
        ycsbb['totalbw'][MOSAICDB] + ycsbb['totalbw'][OLTPIM] + 
        ycsba['totalbw'][MOSAICDB] + ycsba['totalbw'][OLTPIM] + 
        ycsbi['totalbw'][MOSAICDB] + ycsbi['totalbw'][OLTPIM] + 
        ycsbs['totalbw'][MOSAICDB] + ycsbs['totalbw'][OLTPIM]
    )
    def dram_plot(axis, workload, indices, labels, top_ylim=True):
        width = 0.3
        indices1 = [x - width/2 for x in indices]
        indices2 = [x + width/2 for x in indices]
        bottom = [0 for _ in indices]
        axis.bar(indices1, workload['dramrd'][MOSAICDB], width, bottom=bottom, **STYLES['dramrd'])
        bottom = [b + n for b, n in zip(bottom, workload['dramrd'][MOSAICDB])]
        axis.bar(indices1, workload['dramwr'][MOSAICDB], width, bottom=bottom, **STYLES['dramwr'])
        bottom = [0 for _ in indices]
        axis.bar(indices2, workload['dramrd'][OLTPIM], width, bottom=bottom, **STYLES['dramrd'])
        bottom = [b + n for b, n in zip(bottom, workload['dramrd'][OLTPIM])]
        axis.bar(indices2, workload['dramwr'][OLTPIM], width, bottom=bottom, **STYLES['dramwr'])
        bottom = [b + n for b, n in zip(bottom, workload['dramwr'][OLTPIM])]
        axis.bar(indices2, workload['pimrd'][OLTPIM], width, bottom=bottom, **STYLES['pimrd'])
        bottom = [b + n for b, n in zip(bottom, workload['pimrd'][OLTPIM])]
        axis.bar(indices2, workload['pimwr'][OLTPIM], width, bottom=bottom, **STYLES['pimwr'])
        axis.bar(indices1, workload['totalbw'][MOSAICDB], width, **STYLES['mosaicdb']['bar-cover'])
        axis.bar(indices2, workload['totalbw'][OLTPIM], width, **STYLES['oltpim']['bar-cover'])
        axis.set_xticks(indices)
        axis.set_xticklabels(labels)
        axis.set_xlabel('')
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_xlim(-0.5, len(indices) - 0.5)
        if top_ylim: axis.set_ylim(bottom=0, top=dram_ylim * 1.1)
        else: axis.set_ylim(bottom=0)
        axis.grid(axis='y', color='lightgrey', linestyle='--')
        axis.set_axisbelow(True)
    dram_plot(axes[1][0], ycsbc, x_indices, size_labels)
    dram_plot(axes[1][1], ycsbb, x_indices, size_labels)
    dram_plot(axes[1][2], ycsba, x_indices, size_labels)
    dram_plot(axes[1][3], ycsbs, x_indices, ycsbs_labels)
    dram_plot(axes[1][4], ycsbi, x_indices, ycsbi_labels)
    dram_plot(axes[1][5], tpcc, x_indices, tpcc_labels, top_ylim=False)
    axes[1][0].set_ylabel('Memory Traffic\n$\\bf{←}$ (KBPT)  ')
    axes[1][1].set_xlabel('Table Size')
    axes[1][3].set_xlabel('Max Scan Length')
    axes[1][4].set_xlabel('Insert Ratio')
    axes[1][5].set_xlabel('Writes:Reads Ratio')
    axes[1][1].set_yticklabels('')
    axes[1][2].set_yticklabels('')
    axes[1][3].set_yticklabels('')
    axes[1][4].set_yticklabels('')

    legh = [
        (
            mline.Line2D([0], [0], **STYLES['mosaicdb']['plot']),
            mpatch.Patch(**STYLES['mosaicdb']['bar-cover'])
        ),
        (
            mline.Line2D([0], [0], **STYLES['oltpim']['plot']),
            mpatch.Patch(**STYLES['oltpim']['bar-cover'])
        ),
        mpatch.Patch(**STYLES['dramrd']), mpatch.Patch(**STYLES['dramwr']),
        mpatch.Patch(**STYLES['pimrd']), mpatch.Patch(**STYLES['pimwr']),
    ]
    legl = [
        MOSAICDB, OLTPIM, 'DRAM.Rd', 'DRAM.Wr', 'PIM.Rd', 'PIM.Wr',
    ]
    fig.legend(legh, legl, ncol=6, loc='lower center', bbox_to_anchor=(0.5, 1),
        handler_map={tuple: mlegh.HandlerTuple(ndivide=None)})
    fig.align_ylabels()
    plt.savefig(result_plot_path(args, PLOT_NAME), bbox_inches='tight')
    plt.close(fig)

def plot_gc(args):
    EXP_NAME = 'ycsb'
    PLOT_NAME = 'gc'
    SIZE_TO_LABEL = {10**6: '1M', 10**7: '10M', 10**8: '100M', 10**9: '1B'}
    ycsba = {
        True: {
            'size': {MOSAICDB: [], OLTPIM: []},
            'tput': {MOSAICDB: [], OLTPIM: []},
            'p99': {MOSAICDB: [], OLTPIM: []},
            'dramrd': {MOSAICDB: [], OLTPIM: []},
            'dramwr': {MOSAICDB: [], OLTPIM: []},
            'pimrd': {MOSAICDB: [], OLTPIM: []},
            'pimwr': {MOSAICDB: [], OLTPIM: []},
            'totalbw': {MOSAICDB: [], OLTPIM: []},
        },
        False: {
            'size': {MOSAICDB: [], OLTPIM: []},
            'tput': {MOSAICDB: [], OLTPIM: []},
            'p99': {MOSAICDB: [], OLTPIM: []},
            'dramrd': {MOSAICDB: [], OLTPIM: []},
            'dramwr': {MOSAICDB: [], OLTPIM: []},
            'pimrd': {MOSAICDB: [], OLTPIM: []},
            'pimwr': {MOSAICDB: [], OLTPIM: []},
            'totalbw': {MOSAICDB: [], OLTPIM: []},
        },
    }
    def append_to_stats(workload_stats, gc, system, row):
        workload_stats[gc]['size'][system].append(int(row['workload_size']))
        workload_stats[gc]['tput'][system].append(float(row['commits']) / float(row['time(s)']) / 1000000)
        workload_stats[gc]['p99'][system].append(float(row['p99(ms)']))
        per_txn_bw = lambda name: float(row[name]) / float(row['commits']) * (1024*1024*1024/1000000)
        dramrd = per_txn_bw('dram.rd(MiB)')
        dramwr = per_txn_bw('dram.wr(MiB)')
        pimrd = per_txn_bw('pim.rd(MiB)')
        pimwr = per_txn_bw('pim.wr(MiB)')
        workload_stats[gc]['dramrd'][system].append(dramrd)
        workload_stats[gc]['dramwr'][system].append(dramwr)
        workload_stats[gc]['pimrd'][system].append(pimrd)
        workload_stats[gc]['pimwr'][system].append(pimwr)
        workload_stats[gc]['totalbw'][system].append(dramrd + dramwr + pimrd + pimwr)
    for system in SYSTEMS:
        with open(result_file_path(args, EXP_NAME, system), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                system = row['system']
                workload = row['workload']
                gc = (row['GC'] == 'True')
                if workload == 'YCSB-A':
                    append_to_stats(ycsba, gc, system, row)

    fig, axes = plt.subplots(1, 2, figsize=(5, 1.5), constrained_layout=True,
        gridspec_kw={'wspace': 0.1})
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(ycsba[True]['size'][MOSAICDB]))
    x_labels = [SIZE_TO_LABEL[ycsba[True]['size'][MOSAICDB][x]] for x in x_indices]

    def gc_nogc_plot(axis, workload, xlabels, yname):
        axis.plot(x_indices, workload[True][yname][MOSAICDB], label=MOSAICDB, **STYLES['mosaicdb']['plot'])
        axis.plot(x_indices, workload[True][yname][OLTPIM], label=OLTPIM, **STYLES['oltpim']['plot'])
        axis.plot(x_indices, workload[False][yname][MOSAICDB], label=MOSAICDB + ' (no GC)', **STYLES['mosaicdb-light']['plot'])
        axis.plot(x_indices, workload[False][yname][OLTPIM], label=OLTPIM + ' (no GC)', **STYLES['oltpim-light']['plot'])
        axis.set_xticks(x_indices)
        axis.set_xticklabels(xlabels)
        axis.set_xlabel('')
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_xlim(-0.5, len(x_indices) - 0.5)
        axis.set_ylim(bottom=0)
        axis.grid(axis='y', color='lightgrey', linestyle='--')
        axis.set_axisbelow(True)
    gc_nogc_plot(axes[0], ycsba, x_labels, 'tput')
    axes[0].set_ylabel('Throughput\n  (MTPS) $\\bf{→}$')

    slowdown_gen = lambda workload, system: \
        [nogc / gc for gc, nogc in zip(workload[True]['tput'][system], workload[False]['tput'][system])]
    def slowdown_plot(axis, workload, xlabels):
        width = 0.3
        axis.bar([x - width/2 for x in x_indices], slowdown_gen(workload, MOSAICDB), width, label=MOSAICDB, **STYLES['mosaicdb']['bar'])
        axis.bar([x + width/2 for x in x_indices], slowdown_gen(workload, OLTPIM), width, label=OLTPIM, **STYLES['oltpim']['bar'])
        axis.set_xticks(x_indices)
        axis.set_xticklabels(xlabels)
        axis.set_xlabel('')
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_xlim(-0.5, len(x_indices) - 0.5)
        axis.set_ylim(bottom=0)
        axis.grid(axis='y', color='lightgrey', linestyle='--')
        axis.set_axisbelow(True)
    slowdown_plot(axes[1], ycsba, x_labels)
    axes[1].set_ylabel('$\\bf{←}$ GC Slowdown  ')
    fig.supxlabel('Table Size')
    legh0, legl0 = axes[0].get_legend_handles_labels()
    legh1, legl1 = axes[1].get_legend_handles_labels()
    legh = [
        (legh0[0], legh1[0]),
        (legh0[1], legh1[1]),
        legh0[2], legh0[3]
    ]
    fig.legend(legh, legl0, ncol=2, loc='lower center', bbox_to_anchor=(0.5, 1),
        handler_map={tuple: mlegh.HandlerTuple(ndivide=None)})
    fig.align_ylabels()
    plt.savefig(result_plot_path(args, PLOT_NAME), bbox_inches='tight')
    plt.close(fig)

def plot_logging(args):
    EXP_NAME = 'ycsb'
    PLOT_NAME = 'logging'
    RATIO_TO_LABEL = {0.1: '10%', 0.2: '20%', 0.5: '50%', 1.0: '100%'}
    ycsbi = {
        True: {
            'ratio': [],
            'tput': [],
            'p99': [],
            'dramrd': [],
            'dramwr': [],
            'pimrd': [],
            'pimwr': [],
            'totalbw': [],
        },
        False: {
            'ratio': [],
            'tput': [],
            'p99': [],
            'dramrd': [],
            'dramwr': [],
            'pimrd': [],
            'pimwr': [],
            'totalbw': [],
        },
    }
    ycsbu = {
        True: {
            'ratio': [],
            'tput': [],
            'p99': [],
            'dramrd': [],
            'dramwr': [],
            'pimrd': [],
            'pimwr': [],
            'totalbw': [],
        },
        False: {
            'ratio': [],
            'tput': [],
            'p99': [],
            'dramrd': [],
            'dramwr': [],
            'pimrd': [],
            'pimwr': [],
            'totalbw': [],
        },
    }
    def append_to_stats(workload_stats, row):
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
    with open(result_file_path(args, EXP_NAME, OLTPIM), 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            workload = row['workload']
            logging = (row['log'] == 'True')
            if workload == 'YCSB-I1':
                ycsbi[logging]['ratio'].append(0.1)
                append_to_stats(ycsbi[logging], row)
            elif workload == 'YCSB-I2':
                ycsbi[logging]['ratio'].append(0.2)
                append_to_stats(ycsbi[logging], row)
            elif workload == 'YCSB-I3':
                ycsbi[logging]['ratio'].append(0.5)
                append_to_stats(ycsbi[logging], row)
            elif workload == 'YCSB-I4':
                ycsbi[logging]['ratio'].append(1.0)
                append_to_stats(ycsbi[logging], row)
            elif workload == 'YCSB-U1':
                ycsbu[logging]['ratio'].append(0.1)
                append_to_stats(ycsbu[logging], row)
            elif workload == 'YCSB-U2':
                ycsbu[logging]['ratio'].append(0.2)
                append_to_stats(ycsbu[logging], row)
            elif workload == 'YCSB-U3':
                ycsbu[logging]['ratio'].append(0.5)
                append_to_stats(ycsbu[logging], row)
            elif workload == 'YCSB-U4':
                ycsbu[logging]['ratio'].append(1.0)
                append_to_stats(ycsbu[logging], row)

    fig, axes = plt.subplots(3, 2, figsize=(5, 3.5), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(ycsbi[True]['ratio']))
    labels = [RATIO_TO_LABEL[ycsbi[True]['ratio'][x]] for x in x_indices]

    tput_ylim = max(
        ycsbi[True]['tput'] + ycsbi[False]['tput'] +
        ycsbu[True]['tput'] + ycsbu[False]['tput']
    )
    def tput_plot(axis, workload, indices, title):
        axis.plot(indices, workload[True]['tput'], **STYLES['oltpim']['plot'])
        axis.plot(indices, workload[False]['tput'], **STYLES['oltpim-light']['plot'])
        axis.set_xticks(indices)
        axis.set_xticklabels('')
        axis.set_xlabel('')
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_xlim(-0.5, len(indices) - 0.5)
        axis.set_ylim(bottom=0, top=tput_ylim * 1.1)
        axis.title.set_text(title)
        axis.grid(axis='y', color='lightgrey', linestyle='--')
        axis.set_axisbelow(True)
    tput_plot(axes[0][0], ycsbi, x_indices, 'YCSB-I')
    tput_plot(axes[0][1], ycsbu, x_indices, 'YCSB-U')
    axes[0][0].set_ylabel('Throughput\n  (MTPS) $\\bf{→}$', fontsize=9)
    axes[0][1].set_yticklabels('')

    p99_ylim = max(
        ycsbi[True]['p99'] + ycsbi[False]['p99'] +
        ycsbu[True]['p99'] + ycsbu[False]['p99']
    )
    def p99_plot(axis, workload, indices):
        width = 0.3
        indices1 = [x - width/2 for x in indices]
        indices2 = [x + width/2 for x in indices]
        axis.bar(indices1, workload[True]['p99'], width, **STYLES['oltpim']['bar'])
        axis.bar(indices2, workload[False]['p99'], width, **STYLES['oltpim-light']['bar'])
        axis.set_xticks(indices)
        axis.set_xticklabels('')
        axis.set_xlabel('')
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_xlim(-0.5, len(indices) - 0.5)
        axis.set_ylim(bottom=0, top=p99_ylim * 1.1)
        axis.grid(axis='y', color='lightgrey', linestyle='--')
        axis.set_axisbelow(True)
    p99_plot(axes[1][0], ycsbi, x_indices)
    p99_plot(axes[1][1], ycsbu, x_indices)
    axes[1][0].set_ylabel('P99 Latency\n$\\bf{←}$ (ms)  ', fontsize=9)
    axes[1][1].set_yticklabels('')

    dram_ylim = max(
        ycsbi[True]['totalbw'] + ycsbi[False]['totalbw'] +
        ycsbu[True]['totalbw'] + ycsbu[False]['totalbw']
    )
    def dram_plot(axis, workload, indices, labels):
        width = 0.3
        indices1 = [x - width/2 for x in indices]
        indices2 = [x + width/2 for x in indices]
        bottom = [0 for _ in indices]
        axis.bar(indices1, workload[True]['dramrd'], width, bottom=bottom, **STYLES['dramrd'])
        bottom = [b + n for b, n in zip(bottom, workload[True]['dramrd'])]
        axis.bar(indices1, workload[True]['dramwr'], width, bottom=bottom, **STYLES['dramwr'])
        bottom = [b + n for b, n in zip(bottom, workload[True]['dramwr'])]
        axis.bar(indices1, workload[True]['pimrd'], width, bottom=bottom, **STYLES['pimrd'])
        bottom = [b + n for b, n in zip(bottom, workload[True]['pimrd'])]
        axis.bar(indices1, workload[True]['pimwr'], width, bottom=bottom, **STYLES['pimwr'])
        bottom = [0 for _ in indices]
        axis.bar(indices2, workload[False]['dramrd'], width, bottom=bottom, **STYLES['dramrd'])
        bottom = [b + n for b, n in zip(bottom, workload[False]['dramrd'])]
        axis.bar(indices2, workload[False]['dramwr'], width, bottom=bottom, **STYLES['dramwr'])
        bottom = [b + n for b, n in zip(bottom, workload[False]['dramwr'])]
        axis.bar(indices2, workload[False]['pimrd'], width, bottom=bottom, **STYLES['pimrd'])
        bottom = [b + n for b, n in zip(bottom, workload[False]['pimrd'])]
        axis.bar(indices2, workload[False]['pimwr'], width, bottom=bottom, **STYLES['pimwr'])
        axis.bar(indices1, workload[True]['totalbw'], width, **STYLES['oltpim']['bar-cover'])
        axis.bar(indices2, workload[False]['totalbw'], width, **STYLES['oltpim-light']['bar-cover'])
        axis.set_xticks(indices)
        axis.set_xticklabels(labels)
        axis.set_xlabel('')
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_xlim(-0.5, len(indices) - 0.5)
        axis.set_ylim(bottom=0, top=dram_ylim * 1.1)
        axis.grid(axis='y', color='lightgrey', linestyle='--')
        axis.set_axisbelow(True)
    dram_plot(axes[2][0], ycsbi, x_indices, labels)
    dram_plot(axes[2][1], ycsbu, x_indices, labels)
    axes[2][0].set_ylabel('Memory Traffic\n$\\bf{←}$ (KBPT)  ', fontsize=9)
    axes[2][0].set_xlabel('Insert Ratio')
    axes[2][1].set_xlabel('Update Ratio')
    axes[2][1].set_yticklabels('')

    legh = [
        (
            mline.Line2D([0], [0], **STYLES['oltpim']['plot']),
            mpatch.Patch(**STYLES['oltpim']['bar'])
        ),
        (
            mline.Line2D([0], [0], **STYLES['oltpim-light']['plot']),
            mpatch.Patch(**STYLES['oltpim-light']['bar'])
        ),
        mpatch.Patch(**STYLES['dramrd']), mpatch.Patch(**STYLES['dramwr']),
        mpatch.Patch(**STYLES['pimrd']), mpatch.Patch(**STYLES['pimwr']),
    ]
    legl = [
        OLTPIM, OLTPIM + ' (no log)', 'DRAM.Rd', 'DRAM.Wr', 'PIM.Rd', 'PIM.Wr',
    ]
    fig.legend(legh, legl, ncol=3, loc='lower center', bbox_to_anchor=(0.5, 1),
        handler_map={tuple: mlegh.HandlerTuple(ndivide=None)})
    fig.align_ylabels()
    plt.savefig(result_plot_path(args, PLOT_NAME), bbox_inches='tight')
    plt.close(fig)

def plot_skew(args):
    EXP_NAME='ycsb'
    PLOT_NAME='skew'
    ycsbb = {
        'theta': {MOSAICDB: [], OLTPIM: []},
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []},
        'totalbw': {MOSAICDB: [], OLTPIM: []},
    }
    def append_to_stats(workload_stats, system, row):
        workload_stats['theta'][system].append(float(row['YCSBzipfian']))
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
    for system in SYSTEMS:
        with open(result_file_path(args, EXP_NAME, system), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                system = row['system']
                if row['workload'] == 'YCSB-B' and int(row['workload_size']) == 10**9:
                    append_to_stats(ycsbb, system, row)

    fig, axes = plt.subplots(1, 2, figsize=(5, 1.5), constrained_layout=True,
        gridspec_kw={'wspace': 0.1})
    formatter = FuncFormatter(lambda x, _: f'{x:g}')

    def simple_plot(axis, workload, yname):
        width = 0.3
        indices = range(len(workload['theta'][MOSAICDB]))
        labels = [f'{t:g}' for t in workload['theta'][MOSAICDB]]
        axis.bar([x - width/2 for x in indices], workload[yname][MOSAICDB], width, label=MOSAICDB, **STYLES['mosaicdb']['bar'])
        axis.bar([x + width/2 for x in indices], workload[yname][OLTPIM], width, label=OLTPIM, **STYLES['oltpim']['bar'])
        axis.set_xticks(indices)
        axis.set_xticklabels(labels)
        axis.set_xlabel('')
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_xlim(-0.5, len(indices) - 0.5)
        axis.set_ylim(bottom=0)
        axis.grid(axis='y', color='lightgrey', linestyle='--')
        axis.set_axisbelow(True)
    simple_plot(axes[0], ycsbb, 'tput')
    axes[0].set_ylabel('Throughput\n  (MTPS) $\\bf{→}$')
    simple_plot(axes[1], ycsbb, 'totalbw')
    axes[1].set_ylabel('Memory Traffic\n$\\bf{←}$ (KBPT)  ')
    fig.supxlabel('Workload Skewness (θ)')
    legh, legl = axes[0].get_legend_handles_labels()
    fig.legend(legh, legl, ncol=2, loc='lower center', bbox_to_anchor=(0.5, 1))
    fig.align_ylabels()
    plt.savefig(result_plot_path(args, PLOT_NAME), bbox_inches='tight')
    plt.close(fig)

def plot_batchsize(args):
    EXP_NAME = 'batchsize'
    PLOT_NAME = 'batchsize'
    OLTPIM_NOMULTIGET = OLTPIM + ' (no MultiGet)'
    MOSAICDB_OPTIMAL_BS = 8
    OLTPIM_OPTIMAL_BS = 256
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

    fig, axes = plt.subplots(2, 2, figsize=(5, 2.5), constrained_layout=True,
        gridspec_kw={'wspace': 0.1})
    formatter = FuncFormatter(lambda x, _: f'{x:g}')

    def at_optimal_bs(yname, system, bs):
        for b, y in zip(stats['batchsize'][system], stats[yname][system]):
            if b == bs:
                return y
        assert False
    def simple_plot(axis, yname, ylabel, ylog, yticks=None):
        axis.plot(stats['batchsize'][MOSAICDB], stats[yname][MOSAICDB], label=MOSAICDB, **STYLES['mosaicdb']['plot'])
        axis.plot(stats['batchsize'][OLTPIM_NOMULTIGET], stats[yname][OLTPIM_NOMULTIGET], label=OLTPIM_NOMULTIGET, **STYLES['oltpim-light']['plot'])
        axis.plot(stats['batchsize'][OLTPIM], stats[yname][OLTPIM], label=OLTPIM, **STYLES['oltpim']['plot'])
        mosaicdb_at_optimal_bs = at_optimal_bs(yname, MOSAICDB, MOSAICDB_OPTIMAL_BS)
        oltpim_at_optimal_bs = at_optimal_bs(yname, OLTPIM, OLTPIM_OPTIMAL_BS)
        axis.plot([MOSAICDB_OPTIMAL_BS], [mosaicdb_at_optimal_bs], **STYLES['mosaicdb']['plot-highlight'])
        axis.plot([OLTPIM_OPTIMAL_BS], [oltpim_at_optimal_bs], **STYLES['oltpim']['plot-highlight'])
        axis.set_xlabel('')
        axis.set_ylabel(ylabel)
        axis.set_xscale('log')
        if ylog: axis.set_yscale('log')
        else: axis.set_ylim(bottom=0)
        axis.set_xticks([1, 10, 100, 1000])
        if yticks: axis.set_yticks(yticks)
        axis.xaxis.set_major_formatter(formatter)
        axis.yaxis.set_major_formatter(formatter)
        axis.set_xlim(1, 2000)
        axis.minorticks_off()
        axis.grid(axis='y', color='lightgrey', linestyle='--')
        axis.set_axisbelow(True)
    simple_plot(axes[0][0], 'tput', 'Throughput\n  (MTPS) $\\bf{→}$', False, [0, 5, 10])
    simple_plot(axes[0][1], 'p99', 'P99 Latency\n$\\bf{←}$ (ms)  ', True, [0.1, 1, 10, 100])
    simple_plot(axes[1][0], 'totalbw', 'Memory Traffic\n$\\bf{←}$ (KBPT)  ', True)
    simple_plot(axes[1][1], 'abort', 'Abort Rate\n$\\bf{←}$ (%)  ', True)
    axes[0][0].set_xticklabels('')
    axes[0][1].set_xticklabels('')
    axes[1][0].set_ylim(bottom=1)
    axes[1][1].yaxis.set_major_formatter(LogFormatterSciNotation())
    legh, legl = axes[0][0].get_legend_handles_labels()
    fig.supxlabel('Coroutine Batchsize per Thread')
    fig.legend(legh, legl, ncol=3, loc='lower center', bbox_to_anchor=(0.5, 1))
    fig.align_ylabels()
    plt.savefig(result_plot_path(args, PLOT_NAME), bbox_inches='tight')
    plt.close(fig)

def plot_breakdown(args):
    EXP_NAME = 'breakdown'
    PLOT_NAME = 'breakdown'
    stats = {
        'tput': {MOSAICDB: [], OLTPIM: []},
        'p99': {MOSAICDB: [], OLTPIM: []},
        'dramrd': {MOSAICDB: [], OLTPIM: []},
        'dramwr': {MOSAICDB: [], OLTPIM: []},
        'pimrd': {MOSAICDB: [], OLTPIM: []},
        'pimwr': {MOSAICDB: [], OLTPIM: []},
        'totalbw': {MOSAICDB: [], OLTPIM: []},
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
            append_to_stats(OLTPIM, row)

    fig, axes = plt.subplots(1, 1, figsize=(5, 1.5), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    
    #X_LABELS = [
    #    MOSAICDB,
    #    '+PIM\nIndex',
    #    '+PIM\nMVCC',
    #    '+Direct\nAccess', 
    #    '+Inter-\nleave',
    #    MOSAICDB,
    #    OLTPIM,
    #]
    X_LABELS = ['(M)', '(O-3)', '(O-2)', '(O-1)', '(O)', '(MN)', '(ON)']
    x_indices = range(len(X_LABELS))
    x_labels = X_LABELS

    def draw_bar(axis, ydata0, ydata1, ylabel, datalabels):
        width = 0.5
        labelgen = lambda ys: [f"{y:.1f}" for y in ys]
        indices0 = [0, 5]
        indices1 = [1, 2, 3, 4, 6]
        rects0 = axis.bar(indices0, ydata0, width, label=MOSAICDB, **STYLES['mosaicdb']['bar'])
        rects1 = axis.bar(indices1, ydata1, width, label=OLTPIM, **STYLES['oltpim']['bar'])
        if datalabels:
            axis.bar_label(rects0, labels=labelgen(ydata0), padding=3)
            axis.bar_label(rects1, labels=labelgen(ydata1), padding=3)
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
        axis.axvline(4.5, color='black', linestyle='--', linewidth=1)
        axis.grid(axis='y', color='lightgrey', linestyle='--')
        axis.set_axisbelow(True)
    draw_bar(axes, stats['tput'][MOSAICDB], stats['tput'][OLTPIM], 'Throughput\n  (MTPS) $\\bf{→}$', True)
    #draw_bar(axes[1], stats['totalbw'][MOSAICDB], stats['totalbw'][OLTPIM], 'Memory Traffic\n(KBPT)', False, True)
    axes.set_xticklabels(x_labels)
    #axes.set_xticks([5.5], minor=True)
    #axes.set_xticklabels(['\n+NUMA Local'], minor=True)
    #axes.tick_params(axis='x', which='major', labelsize=8)
    #axes.tick_params(axis='x', which='minor', labelsize=8, length=0, pad=8)
    legh, legl = axes.get_legend_handles_labels()
    fig.legend(legh, legl, ncol=2, loc='lower center', bbox_to_anchor=(0.5, 1))
    fig.align_ylabels()
    plt.savefig(result_plot_path(args, PLOT_NAME), bbox_inches='tight')
    plt.close(fig)

if __name__ == "__main__":
    args = parse_args()
    plot_intro(args)
    plot_overall(args)
    plot_gc(args)
    plot_logging(args)
    plot_skew(args)
    plot_batchsize(args)
    plot_breakdown(args)
