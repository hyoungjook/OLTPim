import argparse
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
    parser.add_argument('--hugetlb-size-gb', type=int, default=160,
        help='Size (GiB) of hugeTLB page to pre-allocate.')
    parser.add_argument('--result-dir', type=str, required=True,
        help='Path of directory in which the CSV result file will be stored.')
    parser.add_argument('--bench-seconds', type=int, default=20,
        help='Seconds to run each benchmark.')
    parser.add_argument('--num-upmem-ranks', type=int, required=True,
        help='Total number of UPMEM ranks. Used if system="OLTPim".')
    parser.add_argument('--skip-measure', action='store_false', dest='measure',
        help='Skip measurement and plot graph with latest result')
    args = parser.parse_args()
    return args

def result_file_path(args, exp_name):
    file_name = f'exp_{exp_name}'
    return str(Path(args.result_dir) / file_name)

def result_plot_path(args, exp_name, suffix=''):
    file_name = f'plot_{exp_name}{suffix}.pdf'
    return str(Path(args.result_dir) / file_name)

def create_result_file(args, exp_name):
    result_dir = args.result_dir
    if not os.path.isdir(result_dir):
        os.makedirs(result_dir)
    args.result_file = result_file_path(args, exp_name)

def print_header(args):
    runner = str(Path(__file__).parent / 'evaluate.py')
    cmd = [
        'python3', runner,
        '--result-file', args.result_file,
        '--print-header'
    ]
    subprocess.run(cmd)

def run(args, system, workload, workload_size,
        coro_batch_size=None, no_logging=False, no_hyperthreading=False,
        no_numa_local_workload=False, no_gc=False, no_interleave=False,
        executable_suffix=None):
    runner = str(Path(__file__).parent / 'evaluate.py')
    cmd = [
        'python3', runner,
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
    if no_gc:
        cmd += ['--no-gc']
    if no_interleave:
        cmd += ['--no-interleave']
    if executable_suffix:
        cmd += ['--executable-suffix', executable_suffix]
    subprocess.run(cmd)
