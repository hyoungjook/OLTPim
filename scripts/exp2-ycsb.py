from common import *
import csv
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

EXP_NAME = 'ycsb'
SYSTEMS = ['MosaicDB', 'OLTPim']
WORKLOADS = ['YCSB-C', 'YCSB-A']
WORKLOAD_SIZES = [10 ** 6, 10 ** 7, 10 ** 8, 10 ** 9]
GC_OPTS = {'YCSB-C': [True], 'YCSB-A': [False, True]}

SIZE_TO_LABEL = {10**6: '1M', 10**7: '10M', 10**8: '100M', 10**9: '1B'}

def plot(args):
    MOSAICDB = 'MosaicDB'
    OLTPIM = 'OLTPim'
    ycsbc_size = {MOSAICDB: [], OLTPIM: []}
    ycsbc_tput = {MOSAICDB: [], OLTPIM: []}
    ycsbc_p99 = {MOSAICDB: [], OLTPIM: []}
    ycsba_size = {MOSAICDB: [], OLTPIM: []}
    ycsba_tput = {MOSAICDB: [], OLTPIM: []}
    ycsba_p99 = {MOSAICDB: [], OLTPIM: []}
    ycsba_nogc_size = {MOSAICDB: [], OLTPIM: []}
    ycsba_nogc_tput = {MOSAICDB: [], OLTPIM: []}
    ycsba_nogc_p99 = {MOSAICDB: [], OLTPIM: []}
    with open(result_file_path(args, EXP_NAME), 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            system = row['system']
            workload = row['workload']
            if workload == 'YCSB-C':
                ycsbc_size[system] += [int(row['workload_size'])]
                ycsbc_tput[system] += [float(row['tput(TPS)']) / 1000000]
                ycsbc_p99[system] += [float(row['p99(ms)'])]
            if workload == 'YCSB-A':
                if row['GC'] == 'True':
                    ycsba_size[system] += [int(row['workload_size'])]
                    ycsba_tput[system] += [float(row['tput(TPS)']) / 1000000]
                    ycsba_p99[system] += [float(row['p99(ms)'])]
                else:
                    ycsba_nogc_size[system] += [int(row['workload_size'])]
                    ycsba_nogc_tput[system] += [float(row['tput(TPS)']) / 1000000]
                    ycsba_nogc_p99[system] += [float(row['p99(ms)'])]

    # Readonly performance
    fig, axes = plt.subplots(1, 2, figsize= (5.5, 3), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(ycsbc_size[MOSAICDB]))
    x_labels = [SIZE_TO_LABEL[ycsbc_size[MOSAICDB][x]] for x in x_indices]
    axes[0].plot(x_indices, ycsbc_tput[MOSAICDB], linestyle='-', marker='o', color='black', label=MOSAICDB)
    axes[0].plot(x_indices, ycsbc_tput[OLTPIM], linestyle='-', marker='^', color='red', label=OLTPIM)
    axes[0].set_xticks(x_indices)
    axes[0].set_xticklabels(x_labels)
    axes[0].set_xlabel('')
    axes[0].set_ylabel('Throughput (MTPS)')
    axes[0].yaxis.set_major_formatter(formatter)
    axes[0].minorticks_off()
    axes[0].set_ylim(bottom=0)
    axes[0].legend(loc='lower center')
    axes[1].plot(x_indices, ycsbc_p99[MOSAICDB], linestyle='-', marker='o', color='black', label=MOSAICDB)
    axes[1].plot(x_indices, ycsbc_p99[OLTPIM], linestyle='-', marker='^', color='red', label=OLTPIM)
    axes[1].set_xticks(x_indices)
    axes[1].set_xticklabels(x_labels)
    axes[1].set_xlabel('')
    axes[1].set_ylabel('P99 Latency (ms)')
    axes[1].set_yscale('log')
    axes[1].set_ylim(0.01, 10)
    axes[1].yaxis.set_major_formatter(formatter)
    axes[1].minorticks_off()
    fig.supxlabel('Table Size')
    plt.savefig(result_plot_path(args, EXP_NAME, '-readonly'))
    plt.close(fig)

    # Update
    fig, axes = plt.subplots(1, 2, figsize= (5.5, 3), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(ycsba_size[MOSAICDB]))
    x_labels = [SIZE_TO_LABEL[ycsba_size[MOSAICDB][x]] for x in x_indices]
    axes[0].plot(x_indices, ycsba_nogc_tput[MOSAICDB], linestyle='-', marker='H', color='grey', label=MOSAICDB + ' (no GC)')
    axes[0].plot(x_indices, ycsba_tput[MOSAICDB], linestyle='-', marker='o', color='black', label=MOSAICDB)
    axes[0].plot(x_indices, ycsba_nogc_tput[OLTPIM], linestyle='-', marker='d', color='pink', label=OLTPIM + ' (no GC)')
    axes[0].plot(x_indices, ycsba_tput[OLTPIM], linestyle='-', marker='D', color='red', label=OLTPIM)
    axes[0].set_xticks(x_indices)
    axes[0].set_xticklabels(x_labels)
    axes[0].set_xlabel('')
    axes[0].set_ylabel('Throughput (MTPS)')
    axes[0].yaxis.set_major_formatter(formatter)
    axes[0].minorticks_off()
    axes[0].set_ylim(bottom=0)
    axes[0].legend(loc='lower center')
    axes[1].plot(x_indices, ycsba_nogc_p99[MOSAICDB], linestyle='-', marker='H', color='grey', label=MOSAICDB + ' (no GC)')
    axes[1].plot(x_indices, ycsba_p99[MOSAICDB], linestyle='-', marker='o', color='black', label=MOSAICDB)
    axes[1].plot(x_indices, ycsba_nogc_p99[OLTPIM], linestyle='-', marker='d', color='pink', label=OLTPIM + ' (no GC)')
    axes[1].plot(x_indices, ycsba_p99[OLTPIM], linestyle='-', marker='D', color='red', label=OLTPIM)
    axes[1].set_xticks(x_indices)
    axes[1].set_xticklabels(x_labels)
    axes[1].set_xlabel('')
    axes[1].set_ylabel('P99 Latency (ms)')
    axes[1].set_yscale('log')
    axes[1].set_ylim(0.01, 10)
    axes[1].yaxis.set_major_formatter(formatter)
    axes[1].minorticks_off()
    fig.supxlabel('Table Size')
    plt.savefig(result_plot_path(args, EXP_NAME, '-update'))
    plt.close(fig)

if __name__ == "__main__":
    args = parse_args()
    if args.measure:
        create_result_file(args, EXP_NAME)
        print_header(args)
        for workload in WORKLOADS:
            for workload_size in WORKLOAD_SIZES:
                gc_opts = GC_OPTS[workload]
                for gc in gc_opts:
                    for system in SYSTEMS:
                        run(args, system, workload, workload_size, no_gc=(not gc))
    plot(args)
