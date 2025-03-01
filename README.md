# OLTPim

OLTPim is a research OLTP DBMS implemented on the [UPMEM](https://www.upmem.com/) Processing-in-Memory (PIM) system.
It is implemented by connecting the [OLTPim Engine](https://github.com/hyoungjook/oltpim-engine) with [MosaicDB](https://github.com/sfu-dis/mosaicdb).

## Environment Setup

### Cloning the project
```shell
git clone --recurse-submodules https://github.com/hyoungjook/OLTPim.git
```

### Environment Options
There are two options for running OLTPim:
1. Running on a server equipped with UPMEM DIMMs and [UPMEM SDK](https://sdk.upmem.com) installed. Able to evaluate both PIM version and non-PIM baseline.
2. Running on a traditional server without PIM. Require `-DNO_UPMEM=1` to compile. Able to evaluate only the non-PIM baseline.

### Running on an UPMEM server with Docker
It is tested on the docker container of Ubuntu 22.04 with Linux kernel >= 5.10.
We assume that the host OS has UPMEM SDK 2025.1.0 installed.
```shell
mv scripts/env/Dockerfile.upmem scripts/env/Dockerfile
sudo docker build -t oltpim scripts/env/
```

The docker container should be run with proper arguments to allow UPMEM access from inside the container.
Consult UPMEM for the latest required docker arguments.
The following command worked on our server.
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

### Running on a traditional server without PIM
The non-UPMEM version can be evaluated on a traditional server without PIM.
We assume Ubuntu 22.04 and Linux perf is already installed and working.
```shell
sudo bash scripts/env/setup-cpuonly.sh
```

Or, use the `scripts/env/Dockerfile.cpu` to create the docker container for non-UPMEM version.

## Compile

```shell
mkdir build
cd build
# Choose one from below
cmake -DCMAKE_BUILD_TYPE=Release .. # Release mode
cmake -DCMAKE_BUILD_TYPE=Debug .. # Debug mode
cmake -DCMAKE_BUILD_TYPE=Release -DNO_UPMEM=1 .. # No UPMEM SDK required
make -j`nproc`
cd ..
```

## Reproducing Experiment Results

Run following commands to reproduce the plots. They require sudo privilege or `--privileged` option in docker.

When measuring memory traffic using perf, this project uses hard-coded separation
between DRAM and PIM traffics: we assume DRAMs occupy channels {0, 3} and PIM occupy channels {1, 2, 4, 5}.
If your server has different channel configuration, you should modify the perf-launching code in `benchmarks/bench.cc`.

Also, the launch parameters are hand-optimized to the machine with at least 64 (hyperthreaded) cores and 256GB memory.

For UPMEM server, we assume there are 16 UPMEM DIMMs = 32 UPMEM ranks installed.
If the number of ranks is different, adjust `NUM_UPMEM_RANKS` below; but the code can cause out-of-memory error for the hand-coded experiment settings if not enough ranks are provided.

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

The experiments in the paper are conducted with two servers: OLTPim is evaluated on the UPMEM server and MosaicDB is evaluated on a traditional server.

First, run the above commands with `--systems OLTPim`.

Then, from the traditional server without UPMEM, run:

```shell
mkdir results/
python3 scripts/exp1-batchsize.py --result-dir results/ --systems MosaicDB
python3 scripts/exp2-ycsb.py --result-dir results/ --systems MosaicDB
python3 scripts/exp3-breakdown.py --result-dir results/ --systems MosaicDB
python3 scripts/exp4-tpcc.py --result-dir results/ --systems MosaicDB
```

The files inside the `results/` directory of each server should be gathered into one server for the below plot script to work.

### Generating Plots

After collecting the CSV files in the `results/` directory in the same machine, run:

```shell
python3 scripts/expz-plotall.py --result-dir results/
```
