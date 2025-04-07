# Custom NUMA configurator

This can be used to simulate custom NUMA configuration different from the one OS provides.

Its primary usage is to simulate 4-node partitioning of MosaicDB on a 2-node system.

Since it is developed for limited use case, it has many restrictions:
- Not supports OLTPim (only cpu-only version supported)
- Must include all cpus available in the system
- All cpus in each simulated node should be in the same real numa node.
- If two cpus are hyperthreaded cores from the same physical core, they should be in the same simulated node.

## Usage
```shell
# Input the simulated numa configuration in customnuma/numa_config.h
cd customnuma
bash compile.sh # Makes libnuma.so
cd ..
LD_PRELOAD=customnuma/libnuma.so build/benchmarks/ycsb/ycsb_SI_hybrid_coro --args
```
