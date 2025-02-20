# OLTPim

OLTPim is a research DBMS designed to take advantage of the Processing-in-Memory (PIM) system.
It is implemented by connecting the [OLTPim Engine](https://github.com/hyoungjook/oltpim-engine) with [MosaicDB](https://github.com/sfu-dis/mosaicdb).

## Environment Setup

### Cloning the project
```shell
git clone https://github.com/hyoungjook/OLTPim.git
cd OLTPim
git submodule update --init --recursive
```

### Running with Docker
It is tested on the docker container of Ubuntu 22.04 with Linux kernel >= 5.10.
We assume that the host OS has UPMEM SDK 2025.1.0 installed.
```shell
mv scripts/env/Dockerfile.upmem scripts/env/Dockerfile
sudo docker build -t oltpim scripts/env/
```

You should run the docker container with proper arguments to allow UPMEM access.
Consult UPMEM for the latest required docker arguments.
```shell
DPU_RANKS=$(find /dev/ -type c -name 'dpu_rank*')
RANK_DEVICES=$(for rank in $DPU_RANKS; do echo -n " --device=$rank"; done)

DPU_DAX=$(find /dev/ -type c -name 'dax*.*')
DAX_DEVICES=$(for dax in $DPU_DAX; do echo -n " --device=$dax"; done)

SYS_DPU_VOLUME=" -v /sys/module/dpu:/sys/module/dpu:rw"

DPU_REGIONS=$(find /sys/devices/platform -type d -name 'dpu_region_mem.*')
REGION_VOLUMES=$(for region in $DPU_REGIONS; do echo -n " -v $region:$region:rw"; done)

sudo docker run --rm -it --privileged -v $PWD:/oltpim -w /oltpim \
    $RANK_DEVICES $DAX_DEVICES $SYS_DPU_VOLUME $REGION_VOLUMES \
    oltpim /bin/bash 
```

### Ubuntu 22.04, without UPMEM
You can directly run the non-UPMEM version (the baseline) on Ubuntu 22.04.
You should install Linux perf manually, if not already installed.
```shell
sudo bash scripts/env/setup-cpuonly.sh
```

Or, you can use the `scripts/env/Dockerfile.cpu` to create the docker container for non-UPMEM version.

## Compile

You can build the project in either release or debug mode.

```shell
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release .. # Release mode
cmake -DCMAKE_BUILD_TYPE=Debug .. # Debug mode
make -j`nproc`
cd ..
```

You can build the project for no-UPMEM version, without UPMEM SDK installed.

```shell
cmake -DCMAKE_BUILD_TYPE=Release -DNO_UPMEM=1 .. # Release mode
cmake -DCMAKE_BUILD_TYPE=Debug -DNO_UPMEM=1 .. # Debug mode
```

## Running Experiments

Run following commands to reproduce the plots. They require sudo privilege.

When measuring memory traffic using perf, this project uses hard-coded separation
between DRAM and PIM traffics: we assume DRAMs occupy channels {0, 3} and PIM occupy channels {1, 2, 4, 5}.
If your server has different channel configuration, you should modify the perf-launching code in `benchmarks/bench.cc`.

Also, the launch parameters are hand-optimized to the machine with at least 64 (hyperthreaded) cores and 256GB memory.

```shell
mkdir results/
NUM_UPMEM_RANKS=32 # the number of UPMEM ranks in your system.
python3 scripts/exp1-batchsize.py --result-dir results/ \
    --num-upmem-ranks $NUM_UPMEM_RANKS --systems both --measure-on-upmem-server
python3 scripts/exp2-ycsb.py --result-dir results/ \
    --num-upmem-ranks $NUM_UPMEM_RANKS --systems both --measure-on-upmem-server
python3 scripts/exp3-breakdown.py --result-dir results/ \
    --num-upmem-ranks $NUM_UPMEM_RANKS --systems both --measure-on-upmem-server
python3 scripts/exp4-tpcc.py --result-dir results/ \
    --num-upmem-ranks $NUM_UPMEM_RANKS --systems both --measure-on-upmem-server
```

They will make CSV files in the `results/` directory.

### Running non-UPMEM version in other server

To measure OLTPim and MosaicDB in different servers, run the above commands with `--systems OLTPim`.
Then, from the other server without UPMEM, run:

```shell
mkdir results/
python3 scripts/exp1-batchsize.py --result-dir results/ --systems MosaicDB
python3 scripts/exp2-ycsb.py --result-dir results/ --systems MosaicDB
python3 scripts/exp3-breakdown.py --result-dir results/ --systems MosaicDB
python3 scripts/exp4-tpcc.py --result-dir results/ --systems MosaicDB
```

### Drawing Plots

After collecting the CSV files in the `results/` directory in the same machine, run:

```shell
mkdir results/
python3 scripts/expz-plotall.py --result-dir results/
```
