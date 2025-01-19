import argparse
import datetime
import os
from pathlib import Path
import subprocess

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--build-dir', type=str,
        default=str(Path(__file__).parent.parent / 'build'),
        help='Path of the program build directory.')
    parser.add_argument('--log-dir', type=str, default='/scratch/log',
        help='Path of the logging directory.')
    parser.add_argument('--hugetlb-size-gb', type=int, default=140,
        help='Size (GiB) of hugeTLB page to pre-allocate.')
    parser.add_argument('--result-dir', type=str, required=True,
        help='Path of directory in which the CSV result file will be stored.')
    parser.add_argument('--bench-seconds', type=int, default=20,
        help='Seconds to run each benchmark.')
    parser.add_argument('--num-upmem-ranks', type=int, required=True,
        help='Total number of UPMEM ranks. Used if system="OLTPim".')
    return parser.parse_args()

def create_result_file(args, prefix):
    result_dir = args.result_dir
    if not os.path.isdir(result_dir):
        os.makedirs(result_dir)
    now = datetime.datetime.now()
    now = str(now).replace(' ', '-')
    file_name = f'exp_{prefix}_{args.hugetlb_size_gb}GiB_{args.bench_seconds}s_{now}'
    args.result_file = Path(result_dir) / file_name

def get_executable():
    return str(Path(__file__).parent / 'evaluate.py')

def print_header(args):
    cmd = [
        'python3', get_executable(),
        '--result-file', args.result_file,
        '--print-header'
    ]
    subprocess.run(cmd)

def run(args, system, workload, workload_size,
        coro_batch_size=None, no_logging=False, no_hyperthreading=False,
        no_numa_local_workload=False):
    cmd = [
        'python3', get_executable(),
        '--build-dir', args.build_dir,
        '--log-dir', args.log_dir,
        '--hugetlb-size-gb', str(args.hugetlb_size_gb),
        '--result-file', args.result_file,
        '--system', system,
        '--workload', workload,
        '--workload-size', str(workload_size),
        '--seconds', str(args.bench_seconds),
        '--num-upmem-ranks', str(args.num_upmem_ranks)
    ]
    if coro_batch_size:
        cmd += ['--coro-batch-size', str(coro_batch_size)]
    if no_logging:
        cmd += ['--no-logging']
    if no_hyperthreading:
        cmd += ['--no-hyperthreading']
    if no_numa_local_workload:
        cmd += ['--no-numa-local-workload']
    subprocess.run(cmd)
