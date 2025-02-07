import argparse
import os
from pathlib import Path
import subprocess

MOSAICDB = 'MosaicDB'
OLTPIM = 'OLTPim'

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--build-dir', type=str,
        default=str(Path(__file__).parent.parent / 'build'),
        help='Path of the program build directory.')
    parser.add_argument('--log-dir', type=str, default='/mnt/log',
        help='Path of the logging directory.')
    parser.add_argument('--result-dir', type=str, required=True,
        help='Path of directory in which the CSV result file will be stored.')
    parser.add_argument('--num-upmem-ranks', type=int, default=None,
        help='Total number of UPMEM ranks. Used if system="OLTPim".')
    parser.add_argument('--systems', required=True, choices=[MOSAICDB, OLTPIM, 'both'],
        help='Systems to evaluate. Choices: [MosaicDB, OLTPim, both]')
    parser.add_argument('--measure-on-upmem-server', action='store_true',
        help='Provide if measuring on UPMEM server.')
    args = parser.parse_args()
    args.systems = {
        MOSAICDB: (args.systems == 'MosaicDB' or args.systems == 'both'),
        OLTPIM: (args.systems == 'OLTPim' or args.systems == 'both'),
    }
    if args.systems[OLTPIM] and not args.num_upmem_ranks:
        parser.error('--num-upmem-ranks required for --system OLTPim.')
    return args

def result_file_path(args, exp_name, system):
    file_name = f'exp_{exp_name}_{system.lower()}'
    return str(Path(args.result_dir) / file_name)

def result_plot_path(args, exp_name, suffix=''):
    file_name = f'plot_{exp_name}{suffix}.pdf'
    return str(Path(args.result_dir) / file_name)

def create_result_file(args, exp_name):
    result_dir = args.result_dir
    if not os.path.isdir(result_dir):
        os.makedirs(result_dir)
    args.result_file = dict()
    for system, enabled in args.systems.items():
        if enabled:
            args.result_file[system] = result_file_path(args, exp_name, system)

def print_header(args):
    runner = str(Path(__file__).parent / 'evaluate.py')
    for system, enabled in args.systems.items():
        if enabled:
            cmd = ['python3', runner, '--result-file', args.result_file[system], '--print-header']
            subprocess.run(cmd)

def run(args, system, workload, workload_size,
        bench_seconds, bench_threads, hugetlb_size_gb,
        coro_batch_size=None, no_logging=False,
        no_numa_local_workload=False, no_pim_multiget=False,
        no_gc=False, no_interleave=False, ycsb_zipfian_theta=0,
        executable_suffix=None):
    if not args.systems[system]:
        return
    runner = str(Path(__file__).parent / 'evaluate.py')
    cmd = [
        'python3', runner,
        '--build-dir', args.build_dir,
        '--log-dir', args.log_dir,
        '--hugetlb-size-gb', str(hugetlb_size_gb),
        '--result-file', args.result_file[system],
        '--system', system,
        '--workload', workload,
        '--workload-size', str(workload_size),
        '--seconds', str(bench_seconds),
        '--threads', str(bench_threads),
    ]
    if args.num_upmem_ranks:
        cmd += ['--num-upmem-ranks', str(args.num_upmem_ranks)]
    if args.measure_on_upmem_server:
        cmd += ['--measure-on-upmem-server']
    if coro_batch_size:
        cmd += ['--coro-batch-size', str(coro_batch_size)]
    if no_logging:
        cmd += ['--no-logging']
    if no_numa_local_workload:
        cmd += ['--no-numa-local-workload']
    if no_pim_multiget:
        cmd += ['--no-pim-multiget']
    if no_gc:
        cmd += ['--no-gc']
    if no_interleave:
        cmd += ['--no-interleave']
    if ycsb_zipfian_theta > 0:
        cmd += ['--ycsb-zipfian-theta', str(ycsb_zipfian_theta)]
    if executable_suffix:
        cmd += ['--executable-suffix', executable_suffix]
    ret = subprocess.run(cmd)
    ret.check_returncode()
