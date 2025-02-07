import argparse
import glob
import os
from pathlib import Path
import re
import subprocess
import tempfile

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--build-dir', type=str,
        default=str(Path(__file__).parent.parent / 'build'),
        help='Path of the program build directory.')
    parser.add_argument('--log-dir', type=str, default='/mnt/log',
        help='Path of the logging directory.')
    parser.add_argument('--hugetlb-size-gb', type=int, default=160,
        help='Size (GiB) of hugeTLB page to pre-allocate.')
    parser.add_argument('--log-limit-size-gb', type=int, default=8,
        help='Size (GiB) of log storage. Use 0 to disable log limiting.')
    parser.add_argument('--result-file', type=str, required=True,
        help='CSV result file to append the result.')
    parser.add_argument('--print-header', action='store_true',
        help='Ignore all below, just print the csv header to result file and exit.')

    parser.add_argument('--system', default=None, choices=['MosaicDB', 'OLTPim'],
        help='System to evaluate.')
    parser.add_argument('--workload', default=None, choices=[
        'YCSB-A', 'YCSB-B', 'YCSB-C',
        'YCSB-I1', 'YCSB-I2', 'YCSB-I3', 'YCSB-I4',
        'YCSB-U1', 'YCSB-U2', 'YCSB-U3', 'YCSB-U4',
        'YCSB-S2', 'YCSB-S4', 'YCSB-S8', 'YCSB-S16',
        'TPC-C', 'TPC-CA', 'TPC-CR'
    ], help='Workload to evaluate.')
    parser.add_argument('--workload-size', default=None, type=int,
        help='Table size if YCSB. Scale factor if TPC-C.')
    parser.add_argument('--seconds', type=int, default=60,
        help='Seconds to run the benchmark.')
    parser.add_argument('--threads', type=int, default=os.cpu_count(),
        help='Number of worker threads.')
    parser.add_argument('--coro-batch-size', type=int, default=None,
        help='Coroutine batch size per worker thread.')
    parser.add_argument('--no-logging', action='store_false', dest='logging',
        help='Disable logging')
    parser.add_argument('--no-numa-local-workload', action='store_false', dest='numa_local_workload',
        help='Disable NUMA-local workload.')
    parser.add_argument('--no-pim-multiget', action='store_false', dest='pim_multiget',
        help='Disable PIM multiget transactions.')
    parser.add_argument('--no-gc', action='store_false', dest='gc',
        help='Disable garbage collection.')
    parser.add_argument('--no-interleave', action='store_false', dest='interleave',
        help='Disable PIM-CPU interleaving.')
    parser.add_argument('--executable-suffix', type=str, default=None,
        help='Suffix to executable.')
    parser.add_argument('--num-upmem-ranks', type=int, default=None,
        help='Total number of UPMEM ranks. Used if system="OLTPim".')
    parser.add_argument('--measure-on-upmem-server', action='store_true',
        help='Provide if measuring on UPMEM server.')
    args = parser.parse_args()

    if not args.print_header:
        if not args.system:
            parser.error('--system is a required flag.')
        if not args.workload:
            parser.error('--workload is a required flag.')
        if not args.workload_size:
            parser.error('--workload-size is a required flag.')
    if args.system == 'OLTPim':
        if not args.num_upmem_ranks:
            parser.error('--system=OLTPim requires --num-upmem-ranks flag.')

    return args

def allocate_hugetlb(args):
    # Allocate, unit of 2MiB page
    pages = int(args.hugetlb_size_gb * 1024 / 2)
    with open('/proc/sys/vm/nr_hugepages', 'w') as f:
        f.write(str(pages))
    # Number of numa nodes
    with open('/sys/devices/system/node/online', 'r') as f:
        nodes = f.read().strip().split('-')
        args.numa_nodes = int(nodes[1]) + 1
    # GiB per node
    args.hugetlb_size_gb_per_node = int(args.hugetlb_size_gb // args.numa_nodes)

def cleanup_log_dir(args):
    wrapup_log_dir(args)
    # Ensure tmpfs is mounted
    if args.logging:
        assert not os.path.ismount(args.log_dir)
        mem_for_log = 2 * args.log_limit_size_gb
        r = subprocess.run([
            'mount', '-t', 'tmpfs', 
            '-o', f'size={mem_for_log}G',
            'tmpfs', args.log_dir
        ])
        r.check_returncode()

def wrapup_log_dir(args):
    # Clean up contents
    if os.path.isdir(args.log_dir):
        files = glob.glob(f'{args.log_dir}/*')
        for f in files:
            os.remove(f)
    else:
        os.makedirs(args.log_dir)
    # Unmount if mounted
    if os.path.ismount(args.log_dir):
        r = subprocess.run(['umount', args.log_dir])
        r.check_returncode()

def common_options(args):
    physical_workers_only = 1 if args.threads <= (os.cpu_count() // 2) else 0
    enable_gc = 1 if args.gc else 0
    opts = [
        f'-node_memory_gb={args.hugetlb_size_gb_per_node}',
        f'-threads={args.threads}',
        f'-physical_workers_only={physical_workers_only}',
        f'-seconds={args.seconds}',
        f'-enable_gc={enable_gc}',
        f'-gc_prob=1.0',
        '-arena_size_mb=1',
        '-measure_energy=1'
    ]
    if args.measure_on_upmem_server:
        opts += ['-measure_energy_on_upmem_server=1']
    return opts

def log_options(args):
    opts = [f'-log_data_dir={args.log_dir}']
    if args.logging:
        opts += [
            '-null_log_device=0',
            '-log_direct_io=0',
            '-null_log_during_init=1',
            '-pcommit=0'
        ]
        if args.log_limit_size_gb > 0:
            log_limit_per_thd = float(args.log_limit_size_gb) * 1024.0 / args.threads
            opts += [f'-log_limit_mb={int(log_limit_per_thd)}']
    else:
        opts += [
            '-null_log_device=1',
            '-pcommit=0'
        ]
    return opts

def ycsb_options(args):
    numa_local = 1 if args.numa_local_workload else 0
    table_size = args.workload_size // args.numa_nodes if numa_local else args.workload_size
    match args.workload:
        case 'YCSB-A': ycsb_type = 'A'
        case 'YCSB-B': ycsb_type = 'B'
        case 'YCSB-C': ycsb_type = 'C'
        case 'YCSB-I1': ycsb_type = 'I1'
        case 'YCSB-I2': ycsb_type = 'I2'
        case 'YCSB-I3': ycsb_type = 'I3'
        case 'YCSB-I4': ycsb_type = 'I4'
        case 'YCSB-U1': ycsb_type = 'U1'
        case 'YCSB-U2': ycsb_type = 'U2'
        case 'YCSB-U3': ycsb_type = 'U3'
        case 'YCSB-U4': ycsb_type = 'U4'
        case 'YCSB-S2': ycsb_type = 'S'; ycsb_scan_length = 2
        case 'YCSB-S4': ycsb_type = 'S'; ycsb_scan_length = 4
        case 'YCSB-S8': ycsb_type = 'S'; ycsb_scan_length = 8
        case 'YCSB-S16': ycsb_type = 'S'; ycsb_scan_length = 16
        case _: raise ValueError(f'Invalid workload={args.workload}')
    opts = [
        '-ycsb_ops_per_hot_tx=10',
        '-ycsb_update_per_tx=10',
        '-ycsb_ins_per_tx=10',
        '-ycsb_hot_tx_percent=1.0',
        '-ycsb_read_tx_type=hybrid-coro',
        f'-ycsb_numa_local={numa_local}',
        f'-numa_spread={numa_local}',
        f'-ycsb_hot_table_size={table_size}',
        f'-ycsb_workload={ycsb_type}'
    ]
    if ycsb_type == 'S':
        opts += [f'-ycsb_max_scan_size={ycsb_scan_length}']
    if args.system == 'OLTPim' and args.pim_multiget:
        opts += ['-ycsb_oltpim_multiget=1']
    return opts

def tpcc_options(args):
    numa_local = 1 if args.numa_local_workload else 0
    scale_factor = args.workload_size
    opts = [
        f'-tpcc_numa_local={numa_local}',
        f'-numa_spread={numa_local}',
        f'-tpcc_scale_factor={scale_factor}'
    ]
    if args.workload == 'TPC-CA':
        opts += [
            '-tpcc_atomic_ytd=1'
        ]
    elif args.workload == 'TPC-CR':
        opts += [
            '-tpcc_txn_workload_mix="0,0,0,0,50,50,0,0"'
        ]
    if args.system == 'OLTPim' and args.pim_multiget:
        opts += ['-tpcc_oltpim_multiget=1']
    return opts

def mosaicdb_options(args):
    if not args.coro_batch_size:
        args.coro_batch_size = 8
    opts = [
        f'-coro_batch_size={args.coro_batch_size}',
        '-coro_scheduler=0'
    ]
    return opts

def oltpim_options(args):
    num_ranks_per_numa = args.num_upmem_ranks // args.numa_nodes
    if not args.coro_batch_size:
        args.coro_batch_size = 256
    interleave = 1 if args.interleave else 0
    opts = [
        f'-coro_batch_size={args.coro_batch_size}',
        '-coro_scheduler=1',
        f'-oltpim_num_ranks_per_numa_node={num_ranks_per_numa}',
        f'-oltpim_interleave={interleave}'
    ]
    return opts

def evaluate(args):
    executable = Path(args.build_dir) / 'benchmarks'
    match args.system:
        case 'MosaicDB':
            executable_suffix = 'hybrid_coro'
        case 'OLTPim':
            executable_suffix = 'oltpim'
        case _:
            raise ValueError(f'Invalid system={args.system}')
    if args.executable_suffix:
        executable_suffix = f'{executable_suffix}{args.executable_suffix}'
    if 'YCSB' in args.workload:
        executable = executable / 'ycsb' / f'ycsb_SI_{executable_suffix}'
    elif 'TPC-C' in args.workload:
        executable = executable / 'tpcc' / f'tpcc_SI_{executable_suffix}'
    else:
        raise ValueError(f'Invalid workload={args.workload}')

    cmd = [str(executable)]
    cmd += common_options(args)
    cmd += log_options(args)
    if 'YCSB' in args.workload:
        cmd += ycsb_options(args)
    elif 'TPC-C' in args.workload:
        cmd += tpcc_options(args)
    else:
        raise ValueError(f'Invalid workload={args.workload}')
    if args.system == 'MosaicDB':
        cmd += mosaicdb_options(args)
    elif args.system == 'OLTPim':
        cmd += oltpim_options(args)
    else:
        raise ValueError(f'Invalid system={args.system}')

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as log:
        log_file = log.name
        print(f'Writing execution log to {log_file}')
        log.write(f'CMD: {cmd}\n\n')
        ret = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT)
        ret.check_returncode()
    with open(log_file, 'r') as f:
        result_str = f.read()
    return result_str

def parse_result(result):
    lines = result.split('\n')
    RESULT_PARSE_LINE = '---------------------------------------'
    result_line = None
    for i in range(len(lines)):
        # The first '-----..' indicates to read the next line
        if RESULT_PARSE_LINE in lines[i]:
            result_line = lines[i + 1]
            break
    if not result_line:
        print('Execution failed. Refer to the log.')
        exit(1)
    values = re.split(r'[ ,]', result_line)
    values = list(filter(None, values))
    assert len(values) % 2 == 0
    values_dict = {}
    for i in range(len(values) // 2):
        values_dict[values[2 * i + 1]] = float(values[2 * i])

    return values_dict

def print_header():
    csv_header = 'system,suffix,workload,workload_size,threads,corobatchsize,' + \
        'log,NUMALocal,GC,Interleave,PIMMultiget,' + \
        'time(s),commits,aborts,p99(ms),' + \
        'Epkg(J),Eram(J),' + \
        'dram.rd(MiB),dram.wr(MiB),' + \
        'pim.rd(MiB),pim.wr(MiB),' + \
        'PIMUtil,PIMmramratio,PIMmramsize(B)' + \
        '\n'
    with open(args.result_file, 'w') as f:
        f.write(csv_header)

def print_result(args, values):
    csv = f"{args.system},{args.executable_suffix},{args.workload},{args.workload_size}," + \
        f"{args.threads},{args.coro_batch_size}," + \
        f"{args.logging},{args.numa_local_workload}," + \
        f"{args.gc},{args.interleave},{args.pim_multiget}," + \
        f"{values['time(s)']},{values['commits']},{values['aborts']},{values['p99(ms)']}," + \
        f"{values['Epkg(J)']},{values['Eram(J)']}," + \
        f"{values['dram.rd(MiB)']},{values['dram.wr(MiB)']}," + \
        f"{values['pim.rd(MiB)']},{values['pim.wr(MiB)']}," + \
        f"{values['pim-util']},{values['pim-mram-ratio']},{values['pim-mram-size(B)']}" + \
        "\n"
    with open(args.result_file, 'a') as f:
        f.write(csv)

if __name__ == "__main__":
    args = parse_args()
    if args.print_header:
        print_header()
        exit(0)
    print(args)
    allocate_hugetlb(args)
    cleanup_log_dir(args)
    result = evaluate(args)
    values = parse_result(result)
    print_result(args, values)
    wrapup_log_dir(args)
