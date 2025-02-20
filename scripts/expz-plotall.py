from common import *
import csv
import matplotlib.legend_handler as mlegh
import matplotlib.lines as mline
import matplotlib.patches as mpatch
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogFormatterSciNotation

SYSTEMS = [MOSAICDB, OLTPIM]

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--result-dir', type=str, required=True,
        help='Path of directory where results are stored and plots will be stored.')
    return parser.parse_args()

def plot_intro(args):
    EXP_NAME = 'ycsb'
    PLOT_NAME = 'intro'
    LABELS = ['100/0', '95/5', '50/50']
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
                    if workload in ['YCSB-C', 'YCSB-B', 'YCSB-A']:
                        append_to_stats(system, row)
    assert len(stats_1B['workload'][MOSAICDB]) == len(LABELS) and \
            len(stats_1B['workload'][OLTPIM]) == len(LABELS)

    fig, axes = plt.subplots(1, 2, figsize=(5, 2), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')

    def simple_bar(axis, yname, ylabel):
        width = 0.3
        indices = range(len(LABELS))
        axis.bar([x - width/2 for x in indices], stats_1B[yname][MOSAICDB], width, color='white', edgecolor='black', hatch='\\\\', label=MOSAICDB)
        axis.bar([x + width/2 for x in indices], stats_1B[yname][OLTPIM], width, color='white', edgecolor='red', hatch='//', label=OLTPIM)
        speedups = [f'{o/m:.2f}x' for m, o in zip(stats_1B[yname][MOSAICDB], stats_1B[yname][OLTPIM])]
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
    simple_bar(axes[0], 'tput', 'Throughput (MTPS)')
    simple_bar(axes[1], 'totalbw', 'Memory Traffic (KBPT)')
    fig.supxlabel('Workload Read/Update Ratio')
    legh, legl = axes[0].get_legend_handles_labels()
    fig.legend(legh, legl, ncol=2, loc='lower center', bbox_to_anchor=(0.5, 1))
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
        axis.plot(indices, workload['tput'][MOSAICDB], color='black', linestyle='-', marker='o', label=MOSAICDB)
        axis.plot(indices, workload['tput'][OLTPIM], color='red', linestyle='-', marker='o', label=OLTPIM)
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
    tput_plot(axes[0][1], ycsbs, x_indices, ycsbs_labels, 'YCSB-S')
    tput_plot(axes[0][2], ycsbb, x_indices, size_labels, 'YCSB-B')
    tput_plot(axes[0][3], ycsba, x_indices, size_labels, 'YCSB-A')
    tput_plot(axes[0][4], ycsbi, x_indices, ycsbi_labels, 'YCSB-I')
    tput_plot(axes[0][5], tpcc, x_indices, tpcc_labels, 'TPC-C', top_ylim=False)
    axes[0][0].set_ylabel('Throughput\n(MTPS)')
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
        axis.bar(indices1, workload['dramrd'][MOSAICDB], width, bottom=bottom, color='lightgreen', edgecolor='black')
        bottom = [b + n for b, n in zip(bottom, workload['dramrd'][MOSAICDB])]
        axis.bar(indices1, workload['dramwr'][MOSAICDB], width, bottom=bottom, color='green', edgecolor='black')
        bottom = [0 for _ in indices]
        axis.bar(indices2, workload['dramrd'][OLTPIM], width, bottom=bottom, color='lightgreen', edgecolor='red')
        bottom = [b + n for b, n in zip(bottom, workload['dramrd'][OLTPIM])]
        axis.bar(indices2, workload['dramwr'][OLTPIM], width, bottom=bottom, color='green', edgecolor='red')
        bottom = [b + n for b, n in zip(bottom, workload['dramwr'][OLTPIM])]
        axis.bar(indices2, workload['pimrd'][OLTPIM], width, bottom=bottom, color='lightgrey', edgecolor='red')
        bottom = [b + n for b, n in zip(bottom, workload['pimrd'][OLTPIM])]
        axis.bar(indices2, workload['pimwr'][OLTPIM], width, bottom=bottom, color='grey', edgecolor='red')
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
    dram_plot(axes[1][1], ycsbs, x_indices, ycsbs_labels)
    dram_plot(axes[1][2], ycsbb, x_indices, size_labels)
    dram_plot(axes[1][3], ycsba, x_indices, size_labels)
    dram_plot(axes[1][4], ycsbi, x_indices, ycsbi_labels)
    dram_plot(axes[1][5], tpcc, x_indices, tpcc_labels, top_ylim=False)
    axes[1][0].set_ylabel('Memory Traffic\n(KBPT)')
    axes[1][0].set_xlabel('Table Size')
    axes[1][1].set_xlabel('Max Scan Length')
    axes[1][2].set_xlabel('Table Size')
    axes[1][3].set_xlabel('Table Size')
    axes[1][4].set_xlabel('Insert Ratio')
    axes[1][5].set_xlabel('Writes:Reads Ratio')
    axes[1][1].set_yticklabels('')
    axes[1][2].set_yticklabels('')
    axes[1][3].set_yticklabels('')
    axes[1][4].set_yticklabels('')

    legh = [
        (
            mline.Line2D([0], [0], color='black', marker='o'),
            mpatch.Patch(facecolor='white', edgecolor='black')
        ),
        (
            mline.Line2D([0], [0], color='red', marker='o'),
            mpatch.Patch(facecolor='white', edgecolor='red')
        ),
        mpatch.Patch(color='lightgreen'), mpatch.Patch(color='green'),
        mpatch.Patch(color='lightgrey'), mpatch.Patch(color='grey'),
    ]
    legl = [
        MOSAICDB, OLTPIM, 'DRAM.Rd', 'DRAM.Wr', 'PIM.Rd', 'PIM.Wr',
    ]
    fig.legend(legh, legl, ncol=6, loc='lower center', bbox_to_anchor=(0.5, 1),
        handler_map={tuple: mlegh.HandlerTuple(ndivide=None)})
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

    fig, axes = plt.subplots(1, 2, figsize=(5, 2), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(ycsba[True]['size'][MOSAICDB]))
    x_labels = [SIZE_TO_LABEL[ycsba[True]['size'][MOSAICDB][x]] for x in x_indices]

    def gc_nogc_plot(axis, workload, xlabels, yname):
        axis.plot(x_indices, workload[True][yname][MOSAICDB], color='black', linestyle='-', marker='o', label=MOSAICDB)
        axis.plot(x_indices, workload[True][yname][OLTPIM], color='red', linestyle='-', marker='o', label=OLTPIM)
        axis.plot(x_indices, workload[False][yname][MOSAICDB], color='grey', linestyle='--', marker='*', label=MOSAICDB + ' (no GC)')
        axis.plot(x_indices, workload[False][yname][OLTPIM], color='pink', linestyle='--', marker='*', label=OLTPIM + ' (no GC)')
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
    axes[0].set_ylabel('Throughput (MTPS)')

    slowdown_gen = lambda workload, system: \
        [nogc / gc for gc, nogc in zip(workload[True]['tput'][system], workload[False]['tput'][system])]
    def slowdown_plot(axis, workload, xlabels):
        width = 0.3
        axis.bar([x - width/2 for x in x_indices], slowdown_gen(workload, MOSAICDB), width, color='white', edgecolor='black', hatch='\\\\', label=MOSAICDB)
        axis.bar([x + width/2 for x in x_indices], slowdown_gen(workload, OLTPIM), width, color='white', edgecolor='red', hatch='//', label=OLTPIM)
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
    axes[1].set_ylabel('GC Slowdown')
    fig.supxlabel('Table Size')
    legh0, legl0 = axes[0].get_legend_handles_labels()
    legh1, legl1 = axes[1].get_legend_handles_labels()
    fig.legend(legh0 + legh1, legl0 + legl1, ncol=3, loc='lower center', bbox_to_anchor=(0.5, 1))
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

    fig, axes = plt.subplots(2, 2, figsize=(5, 3), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(ycsbi[True]['ratio']))
    labels = [RATIO_TO_LABEL[ycsbi[True]['ratio'][x]] for x in x_indices]

    p99_ylim = max(
        ycsbi[True]['p99'] + ycsbi[False]['p99'] +
        ycsbu[True]['p99'] + ycsbu[False]['p99']
    )
    def p99_plot(axis, workload, indices, title):
        width = 0.3
        indices1 = [x - width/2 for x in indices]
        indices2 = [x + width/2 for x in indices]
        axis.bar(indices1, workload[True]['p99'], width, color='white', edgecolor='red', hatch='//')
        axis.bar(indices2, workload[False]['p99'], width, color='white', edgecolor='pink', hatch='xx')
        axis.set_xticks(indices)
        axis.set_xticklabels('')
        axis.set_xlabel('')
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_xlim(-0.5, len(indices) - 0.5)
        axis.set_ylim(bottom=0, top=p99_ylim * 1.1)
        axis.title.set_text(title)
        axis.grid(axis='y', color='lightgrey', linestyle='--')
        axis.set_axisbelow(True)
    p99_plot(axes[0][0], ycsbi, x_indices, 'YCSB-I')
    p99_plot(axes[0][1], ycsbu, x_indices, 'YCSB-U')
    axes[0][0].set_ylabel('P99 Latency\n(ms)')
    axes[0][1].set_yticklabels('')

    dram_ylim = max(
        ycsbi[True]['totalbw'] + ycsbi[False]['totalbw'] +
        ycsbu[True]['totalbw'] + ycsbu[False]['totalbw']
    )
    def dram_plot(axis, workload, indices, labels):
        width = 0.3
        indices1 = [x - width/2 for x in indices]
        indices2 = [x + width/2 for x in indices]
        bottom = [0 for _ in indices]
        axis.bar(indices1, workload[True]['dramrd'], width, bottom=bottom, color='lightgreen', edgecolor='red')
        bottom = [b + n for b, n in zip(bottom, workload[True]['dramrd'])]
        axis.bar(indices1, workload[True]['dramwr'], width, bottom=bottom, color='green', edgecolor='red')
        bottom = [b + n for b, n in zip(bottom, workload[True]['dramwr'])]
        axis.bar(indices1, workload[True]['pimrd'], width, bottom=bottom, color='lightgrey', edgecolor='red')
        bottom = [b + n for b, n in zip(bottom, workload[True]['pimrd'])]
        axis.bar(indices1, workload[True]['pimwr'], width, bottom=bottom, color='grey', edgecolor='red')
        bottom = [0 for _ in indices]
        axis.bar(indices2, workload[False]['dramrd'], width, bottom=bottom, color='lightgreen', edgecolor='pink')
        bottom = [b + n for b, n in zip(bottom, workload[False]['dramrd'])]
        axis.bar(indices2, workload[False]['dramwr'], width, bottom=bottom, color='green', edgecolor='pink')
        bottom = [b + n for b, n in zip(bottom, workload[False]['dramwr'])]
        axis.bar(indices2, workload[False]['pimrd'], width, bottom=bottom, color='lightgrey', edgecolor='pink')
        bottom = [b + n for b, n in zip(bottom, workload[False]['pimrd'])]
        axis.bar(indices2, workload[False]['pimwr'], width, bottom=bottom, color='grey', edgecolor='pink')
        axis.set_xticks(indices)
        axis.set_xticklabels(labels)
        axis.set_xlabel('')
        axis.yaxis.set_major_formatter(formatter)
        axis.minorticks_off()
        axis.set_xlim(-0.5, len(indices) - 0.5)
        axis.set_ylim(bottom=0, top=dram_ylim * 1.1)
        axis.grid(axis='y', color='lightgrey', linestyle='--')
        axis.set_axisbelow(True)
    dram_plot(axes[1][0], ycsbi, x_indices, labels)
    dram_plot(axes[1][1], ycsbu, x_indices, labels)
    axes[1][0].set_ylabel('Memory Traffic\n(KBPT)')
    axes[1][0].set_xlabel('Insert Ratio')
    axes[1][1].set_xlabel('Update Ratio')
    axes[1][1].set_yticklabels('')

    legh = [
        mpatch.Patch(facecolor='white', edgecolor='red', hatch='//'),
        mpatch.Patch(facecolor='white', edgecolor='pink', hatch='xx'),
        mpatch.Patch(color='lightgreen'), mpatch.Patch(color='green'),
        mpatch.Patch(color='lightgrey'), mpatch.Patch(color='grey'),
    ]
    legl = [
        OLTPIM, OLTPIM + ' (no log)', 'DRAM.Rd', 'DRAM.Wr', 'PIM.Rd', 'PIM.Wr',
    ]
    fig.legend(legh, legl, ncol=3, loc='lower center', bbox_to_anchor=(0.5, 1))
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

    fig, axes = plt.subplots(1, 2, figsize=(5, 2), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')

    def simple_plot(axis, workload, yname):
        width = 0.3
        indices = range(len(workload['theta'][MOSAICDB]))
        labels = [f'{t:g}' for t in workload['theta'][MOSAICDB]]
        axis.bar([x - width/2 for x in indices], workload[yname][MOSAICDB], width, color='white', edgecolor='black', hatch='\\\\', label=MOSAICDB)
        axis.bar([x + width/2 for x in indices], workload[yname][OLTPIM], width, color='white', edgecolor='red', hatch='//', label=OLTPIM)
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
    axes[0].set_ylabel('Throughput (MTPS)')
    simple_plot(axes[1], ycsbb, 'totalbw')
    axes[1].set_ylabel('Memory Traffic (KBPT)')
    fig.supxlabel('Workload Skewness (θ)')
    legh, legl = axes[0].get_legend_handles_labels()
    fig.legend(legh, legl, ncol=2, loc='lower center', bbox_to_anchor=(0.5, 1))
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

    fig, axes = plt.subplots(2, 2, figsize=(5, 3), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')

    def at_optimal_bs(yname, system, bs):
        for b, y in zip(stats['batchsize'][system], stats[yname][system]):
            if b == bs:
                return y
        assert False
    def simple_plot(axis, yname, ylabel, ylog, latency_set_yticks=False):
        axis.plot(stats['batchsize'][MOSAICDB], stats[yname][MOSAICDB], linestyle='-', marker='o', color='black', label=MOSAICDB)
        axis.plot(stats['batchsize'][OLTPIM_NOMULTIGET], stats[yname][OLTPIM_NOMULTIGET], linestyle='--', marker='o', color='pink', label=OLTPIM_NOMULTIGET)
        axis.plot(stats['batchsize'][OLTPIM], stats[yname][OLTPIM], linestyle='-', marker='o', color='red', label=OLTPIM)
        mosaicdb_at_optimal_bs = at_optimal_bs(yname, MOSAICDB, MOSAICDB_OPTIMAL_BS)
        oltpim_at_optimal_bs = at_optimal_bs(yname, OLTPIM, OLTPIM_OPTIMAL_BS)
        axis.plot([MOSAICDB_OPTIMAL_BS], [mosaicdb_at_optimal_bs], marker='*', color='black', markersize=15)
        axis.plot([OLTPIM_OPTIMAL_BS], [oltpim_at_optimal_bs], marker='*', color='red', markersize=15)
        axis.set_xlabel('')
        axis.set_ylabel(ylabel)
        axis.set_xscale('log')
        if ylog: axis.set_yscale('log')
        else: axis.set_ylim(bottom=0)
        axis.set_xticks([1, 10, 100, 1000])
        if latency_set_yticks:
            axis.set_yticks([0.1, 1, 10, 100])
        axis.xaxis.set_major_formatter(formatter)
        axis.yaxis.set_major_formatter(formatter)
        axis.set_xlim(1, 2000)
        axis.minorticks_off()
        axis.grid(axis='y', color='lightgrey', linestyle='--')
        axis.set_axisbelow(True)
    simple_plot(axes[0][0], 'tput', 'Throughput\n(MTPS)', False)
    simple_plot(axes[0][1], 'p99', 'P99 Latency (ms)', True, True)
    simple_plot(axes[1][0], 'totalbw', 'Memory Traffic\n(KBPT)', False)
    simple_plot(axes[1][1], 'abort', 'Abort Rate (%)', True)
    axes[0][0].set_xticklabels('')
    axes[0][1].set_xticklabels('')
    bw_max = max(stats['totalbw'][MOSAICDB] + stats['totalbw'][OLTPIM])
    axes[1][0].set_ylim(top=bw_max * 1.2)
    axes[1][1].yaxis.set_major_formatter(LogFormatterSciNotation())
    legh, legl = axes[0][0].get_legend_handles_labels()
    fig.supxlabel('Coroutine Batchsize per Thread')
    fig.legend(legh, legl, ncol=3, loc='lower center', bbox_to_anchor=(0.5, 1))
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

    fig, axes = plt.subplots(1, 1, figsize=(5, 2), constrained_layout=True)
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
    X_LABELS = ['(A)', '(B)', '(C)', '(D)', '(E)', '(AN)', '(EN)']
    x_indices = range(len(X_LABELS))
    x_labels = X_LABELS

    def draw_bar(axis, ydata0, ydata1, ylabel, datalabels):
        width = 0.5
        labelgen = lambda ys: [f"{y:.1f}" for y in ys]
        indices00 = [0]
        indices01 = [5]
        indices10 = [1, 2, 3, 4]
        indices11 = [6]
        rects00 = axis.bar(indices00, ydata0[0:1], width, color='white', edgecolor='black', hatch='\\\\', label=MOSAICDB)
        rects10 = axis.bar(indices10, ydata1[0:4], width, color='white', edgecolor='red', hatch='//', label=OLTPIM)
        rects01 = axis.bar(indices01, ydata0[1:2], width, color='white', edgecolor='black', hatch='\\\\\\\\', label=MOSAICDB + ' (NUMA-partitioned)')
        rects11 = axis.bar(indices11, ydata1[4:5], width, color='white', edgecolor='red', hatch='////', label=OLTPIM + ' (NUMA-partitioned)')
        if datalabels:
            axis.bar_label(rects00, labels=labelgen(ydata0[0:1]), padding=3)
            axis.bar_label(rects10, labels=labelgen(ydata1[0:4]), padding=3)
            axis.bar_label(rects01, labels=labelgen(ydata0[1:2]), padding=3)
            axis.bar_label(rects11, labels=labelgen(ydata1[4:5]), padding=3)
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
    draw_bar(axes, stats['tput'][MOSAICDB], stats['tput'][OLTPIM], 'Throughput\n(MTPS)', True)
    #draw_bar(axes[1], stats['totalbw'][MOSAICDB], stats['totalbw'][OLTPIM], 'Memory Traffic\n(KBPT)', False, True)
    axes.set_xticklabels(x_labels)
    #axes.set_xticks([5.5], minor=True)
    #axes.set_xticklabels(['\n+NUMA Local'], minor=True)
    #axes.tick_params(axis='x', which='major', labelsize=8)
    #axes.tick_params(axis='x', which='minor', labelsize=8, length=0, pad=8)
    legh, legl = axes.get_legend_handles_labels()
    fig.legend(legh, legl, ncol=2, loc='lower center', bbox_to_anchor=(0.5, 1))
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
