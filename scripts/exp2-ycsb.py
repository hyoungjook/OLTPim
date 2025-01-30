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

SIZE_TO_LABEL = {10**6: '1M', 10**7: '10M', 10**8: '100M', 10**9: '1B'}

def plot(args):
    ycsbc_size = {MOSAICDB: [], OLTPIM: []}
    ycsbc_tput = {MOSAICDB: [], OLTPIM: []}
    ycsbc_p99 = {MOSAICDB: [], OLTPIM: []}
    ycsbc_dramrd = {MOSAICDB: [], OLTPIM: []}
    ycsbc_dramwr = {MOSAICDB: [], OLTPIM: []}
    ycsbc_pimrd = {MOSAICDB: [], OLTPIM: []}
    ycsbc_pimwr = {MOSAICDB: [], OLTPIM: []}
    ycsba_size = {MOSAICDB: [], OLTPIM: []}
    ycsba_tput = {MOSAICDB: [], OLTPIM: []}
    ycsba_p99 = {MOSAICDB: [], OLTPIM: []}
    ycsba_dramrd = {MOSAICDB: [], OLTPIM: []}
    ycsba_dramwr = {MOSAICDB: [], OLTPIM: []}
    ycsba_pimrd = {MOSAICDB: [], OLTPIM: []}
    ycsba_pimwr = {MOSAICDB: [], OLTPIM: []}
    ycsba_nogc_size = {MOSAICDB: [], OLTPIM: []}
    ycsba_nogc_tput = {MOSAICDB: [], OLTPIM: []}
    ycsba_nogc_p99 = {MOSAICDB: [], OLTPIM: []}
    for system in SYSTEMS:
        with open(result_file_path(args, EXP_NAME, system), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                system = row['system']
                workload = row['workload']
                if workload == 'YCSB-C':
                    ycsbc_size[system] += [int(row['workload_size'])]
                    ycsbc_tput[system] += [float(row['tput(TPS)']) / 1000000]
                    ycsbc_p99[system] += [float(row['p99(ms)'])]
                    ycsbc_dramrd[system] += [float(row['BWdram.rd(MiB/s)']) / 1024]
                    ycsbc_dramwr[system] += [float(row['BWdram.wr(MiB/s)']) / 1024]
                    ycsbc_pimrd[system] += [float(row['BWpim.rd(MiB/s)']) / 1024]
                    ycsbc_pimwr[system] += [float(row['BWpim.wr(MiB/s)']) / 1024]
                if workload == 'YCSB-A':
                    if row['GC'] == 'True':
                        ycsba_size[system] += [int(row['workload_size'])]
                        ycsba_tput[system] += [float(row['tput(TPS)']) / 1000000]
                        ycsba_p99[system] += [float(row['p99(ms)'])]
                        ycsba_dramrd[system] += [float(row['BWdram.rd(MiB/s)']) / 1024]
                        ycsba_dramwr[system] += [float(row['BWdram.wr(MiB/s)']) / 1024]
                        ycsba_pimrd[system] += [float(row['BWpim.rd(MiB/s)']) / 1024]
                        ycsba_pimwr[system] += [float(row['BWpim.wr(MiB/s)']) / 1024]
                    else:
                        ycsba_nogc_size[system] += [int(row['workload_size'])]
                        ycsba_nogc_tput[system] += [float(row['tput(TPS)']) / 1000000]
                        ycsba_nogc_p99[system] += [float(row['p99(ms)'])]

    # Readonly performance
    fig, axes = plt.subplots(2, 2, figsize= (5.5, 4), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(ycsbc_size[MOSAICDB]))
    x_labels = [SIZE_TO_LABEL[ycsbc_size[MOSAICDB][x]] for x in x_indices]
    width=0.25
    mosaic_indices = [x - width/2 for x in x_indices]
    oltpim_indices = [x + width/2 for x in x_indices]
    axes[0][0].plot(x_indices, ycsbc_tput[MOSAICDB], color='black', linestyle='-', marker='o', label=MOSAICDB)
    axes[0][0].plot(x_indices, ycsbc_tput[OLTPIM], color='red', linestyle='-', marker='o', label=OLTPIM)
    axes[0][0].set_xticks(x_indices)
    axes[0][0].set_xticklabels(x_labels)
    axes[0][0].set_xlabel('')
    axes[0][0].set_ylabel('Throughput (MTPS)')
    axes[0][0].yaxis.set_major_formatter(formatter)
    axes[0][0].minorticks_off()
    axes[0][0].set_ylim(bottom=0)
    axes[0][0].legend(loc='lower center')
    axes[0][1].bar(mosaic_indices, ycsbc_p99[MOSAICDB], width, color='white', edgecolor='black', hatch='\\\\', label=MOSAICDB)
    axes[0][1].bar(oltpim_indices, ycsbc_p99[OLTPIM], width, color='white', edgecolor='red', hatch='//', label=OLTPIM)
    axes[0][1].set_xticks(x_indices)
    axes[0][1].set_xticklabels(x_labels)
    axes[0][1].set_xlabel('')
    axes[0][1].set_ylabel('P99 Latency (ms)')
    axes[0][1].set_yscale('log')
    axes[0][1].yaxis.set_major_formatter(formatter)
    axes[0][1].minorticks_off()
    axes[0][1].legend(loc='center')
    bottoms = [0 for _ in x_indices]
    axes[1][0].bar(mosaic_indices, ycsbc_dramrd[MOSAICDB], width, bottom=bottoms, color='lightgreen', edgecolor='black', hatch='\\\\')
    bottoms = [b + n for b, n in zip(bottoms, ycsbc_dramrd[MOSAICDB])]
    axes[1][0].bar(mosaic_indices, ycsbc_dramwr[MOSAICDB], width, bottom=bottoms, color='green', edgecolor='black', hatch='\\\\')
    bottoms = [0 for _ in x_indices]
    axes[1][0].bar(oltpim_indices, ycsbc_dramrd[OLTPIM], width, bottom=bottoms, color='lightgreen', edgecolor='red', hatch='//')
    bottoms = [b + n for b, n in zip(bottoms, ycsbc_dramrd[OLTPIM])]
    axes[1][0].bar(oltpim_indices, ycsbc_dramwr[OLTPIM], width, bottom=bottoms, color='green', edgecolor='red', hatch='//')
    bottoms = [b + n for b, n in zip(bottoms, ycsbc_dramwr[OLTPIM])]
    axes[1][0].bar(oltpim_indices, ycsbc_pimrd[OLTPIM], width, bottom=bottoms, color='lightgrey', edgecolor='red', hatch='//')
    bottoms = [b + n for b, n in zip(bottoms, ycsbc_pimrd[OLTPIM])]
    axes[1][0].bar(oltpim_indices, ycsbc_pimwr[OLTPIM], width, bottom=bottoms, color='grey', edgecolor='red', hatch='//')
    axes[1][0].set_xticks(x_indices)
    axes[1][0].set_xticklabels(x_labels)
    axes[1][0].set_xlabel('')
    axes[1][0].set_ylabel('Far-Memory Bandwidth (GiB/s)')
    axes[1][0].set_xlim(-0.5, len(x_indices) - 0.5)
    axes[1][0].yaxis.set_major_formatter(formatter)
    axes[1][0].minorticks_off()
    axes[1][0].set_ylim(bottom=0)
    patch_handles = [
        mpatch.Patch(color='lightgreen', label='DRAM.Rd'),
        mpatch.Patch(color='green', label='DRAM.Wr'),
        mpatch.Patch(color='lightgrey', label='PIM.Rd'),
        mpatch.Patch(color='grey', label='PIM.Wr')
    ]
    axes[1][0].legend(handles=patch_handles, loc='upper left')
    ycsbc_tpbw = {
        MOSAICDB: [1000.0 * mtps / (mbps0 + mbps1) for mtps, mbps0, mbps1 in zip(
            ycsbc_tput[MOSAICDB], ycsbc_dramrd[MOSAICDB], ycsbc_dramwr[MOSAICDB]
        )],
        OLTPIM: [1000.0 * mtps / (mbps0 + mbps1 + mbps2 + mbps3) for mtps, mbps0, mbps1, mbps2, mbps3 in zip(
            ycsbc_tput[OLTPIM], ycsbc_dramrd[OLTPIM], ycsbc_dramwr[OLTPIM], ycsbc_pimrd[OLTPIM], ycsbc_pimwr[OLTPIM]
        )]
    }
    axes[1][1].bar(mosaic_indices, ycsbc_tpbw[MOSAICDB], width, color='white', edgecolor='black', hatch='\\\\', label=MOSAICDB)
    axes[1][1].bar(oltpim_indices, ycsbc_tpbw[OLTPIM], width, color='white', edgecolor='red', hatch='//', label=OLTPIM)
    axes[1][1].set_xticks(x_indices)
    axes[1][1].set_xticklabels(x_labels)
    axes[1][1].set_xlabel('')
    axes[1][1].set_ylabel('Txn per Traffic (KTxn/MiB)')
    axes[1][1].set_xlim(-0.5, len(x_indices) - 0.5)
    axes[1][1].yaxis.set_major_formatter(formatter)
    axes[1][1].minorticks_off()
    axes[1][1].set_ylim(bottom=0)
    fig.supxlabel('Table Size')
    plt.savefig(result_plot_path(args, EXP_NAME, '-readonly'))
    plt.close(fig)

    # Update
    fig, axes = plt.subplots(2, 2, figsize= (5.5, 4), constrained_layout=True)
    formatter = FuncFormatter(lambda x, _: f'{x:g}')
    x_indices = range(len(ycsba_size[MOSAICDB]))
    x_labels = [SIZE_TO_LABEL[ycsba_size[MOSAICDB][x]] for x in x_indices]
    axes[0][0].plot(x_indices, ycsba_nogc_tput[MOSAICDB], color='grey', linestyle='--', marker='*', label=MOSAICDB + ' (no GC)')
    axes[0][0].plot(x_indices, ycsba_tput[MOSAICDB], color='black', linestyle='-', marker='o', label=MOSAICDB)
    axes[0][0].plot(x_indices, ycsba_nogc_tput[OLTPIM], color='pink', linestyle='--', marker='*', label=OLTPIM + ' (no GC)')
    axes[0][0].plot(x_indices, ycsba_tput[OLTPIM], color='red', linestyle='-', marker='o', label=OLTPIM)
    axes[0][0].set_xticks(x_indices)
    axes[0][0].set_xticklabels(x_labels)
    axes[0][0].set_xlabel('')
    axes[0][0].set_ylabel('Throughput (MTPS)')
    axes[0][0].yaxis.set_major_formatter(formatter)
    axes[0][0].minorticks_off()
    axes[0][0].set_ylim(bottom=0)
    axes[0][0].legend(loc='lower center')
    axes[0][1].bar(mosaic_indices, ycsba_p99[MOSAICDB], width, color='white', edgecolor='black', hatch='\\\\', label=MOSAICDB)
    axes[0][1].bar(oltpim_indices, ycsba_p99[OLTPIM], width, color='white', edgecolor='red', hatch='//', label=OLTPIM)
    axes[0][1].set_xticks(x_indices)
    axes[0][1].set_xticklabels(x_labels)
    axes[0][1].set_xlabel('')
    axes[0][1].set_ylabel('P99 Latency (ms)')
    axes[0][1].set_yscale('log')
    axes[0][1].yaxis.set_major_formatter(formatter)
    axes[0][1].minorticks_off()
    axes[0][1].legend(loc='center')
    bottoms = [0 for _ in x_indices]
    axes[1][0].bar(mosaic_indices, ycsba_dramrd[MOSAICDB], width, bottom=bottoms, color='lightgreen', edgecolor='black', hatch='\\\\')
    bottoms = [b + n for b, n in zip(bottoms, ycsba_dramrd[MOSAICDB])]
    axes[1][0].bar(mosaic_indices, ycsba_dramwr[MOSAICDB], width, bottom=bottoms, color='green', edgecolor='black', hatch='\\\\')
    bottoms = [0 for _ in x_indices]
    axes[1][0].bar(oltpim_indices, ycsba_dramrd[OLTPIM], width, bottom=bottoms, color='lightgreen', edgecolor='red', hatch='//')
    bottoms = [b + n for b, n in zip(bottoms, ycsba_dramrd[OLTPIM])]
    axes[1][0].bar(oltpim_indices, ycsba_dramwr[OLTPIM], width, bottom=bottoms, color='green', edgecolor='red', hatch='//')
    bottoms = [b + n for b, n in zip(bottoms, ycsba_dramwr[OLTPIM])]
    axes[1][0].bar(oltpim_indices, ycsba_pimrd[OLTPIM], width, bottom=bottoms, color='lightgrey', edgecolor='red', hatch='//')
    bottoms = [b + n for b, n in zip(bottoms, ycsba_pimrd[OLTPIM])]
    axes[1][0].bar(oltpim_indices, ycsba_pimwr[OLTPIM], width, bottom=bottoms, color='grey', edgecolor='red', hatch='//')
    axes[1][0].set_xticks(x_indices)
    axes[1][0].set_xticklabels(x_labels)
    axes[1][0].set_xlabel('')
    axes[1][0].set_ylabel('Far-Memory Bandwidth (GiB/s)')
    axes[1][0].set_xlim(-0.5, len(x_indices) - 0.5)
    axes[1][0].yaxis.set_major_formatter(formatter)
    axes[1][0].minorticks_off()
    axes[1][0].set_ylim(bottom=0)
    patch_handles = [
        mpatch.Patch(color='lightgreen', label='DRAM.Rd'),
        mpatch.Patch(color='green', label='DRAM.Wr'),
        mpatch.Patch(color='lightgrey', label='PIM.Rd'),
        mpatch.Patch(color='grey', label='PIM.Wr')
    ]
    axes[1][0].legend(handles=patch_handles, loc='upper left')
    ycsba_tpbw = {
        MOSAICDB: [1000.0 * mtps / (mbps0 + mbps1) for mtps, mbps0, mbps1 in zip(
            ycsba_tput[MOSAICDB], ycsba_dramrd[MOSAICDB], ycsba_dramwr[MOSAICDB]
        )],
        OLTPIM: [1000.0 * mtps / (mbps0 + mbps1 + mbps2 + mbps3) for mtps, mbps0, mbps1, mbps2, mbps3 in zip(
            ycsba_tput[OLTPIM], ycsba_dramrd[OLTPIM], ycsba_dramwr[OLTPIM], ycsba_pimrd[OLTPIM], ycsba_pimwr[OLTPIM]
        )]
    }
    axes[1][1].bar(mosaic_indices, ycsba_tpbw[MOSAICDB], width, color='white', edgecolor='black', hatch='\\\\', label=MOSAICDB)
    axes[1][1].bar(oltpim_indices, ycsba_tpbw[OLTPIM], width, color='white', edgecolor='red', hatch='//', label=OLTPIM)
    axes[1][1].set_xticks(x_indices)
    axes[1][1].set_xticklabels(x_labels)
    axes[1][1].set_xlabel('')
    axes[1][1].set_ylabel('Txn per Traffic (KTxn/MiB)')
    axes[1][1].set_xlim(-0.5, len(x_indices) - 0.5)
    axes[1][1].yaxis.set_major_formatter(formatter)
    axes[1][1].minorticks_off()
    axes[1][1].set_ylim(bottom=0)
    fig.supxlabel('Table Size')
    plt.savefig(result_plot_path(args, EXP_NAME, '-update'))
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
                        run(args, system, workload, workload_size, no_gc=(not gc))
    else:
        plot(args)
