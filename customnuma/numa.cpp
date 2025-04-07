#include <iostream>
#include <string>
#include <fstream>
#include <thread>
#include <cstdlib>
#include <cassert>
#include <errno.h>
#include <dlfcn.h>
#include "numa_config.h"

#define __API_FUNC__ extern "C"
#define DL_ERROR_IF(cond) \
  if (cond) {fprintf(stderr, "%s\n", dlerror()); abort();}
#define ALWAYS_ASSERT(cond, msg) \
  if (!(cond)) {fprintf(stderr, "%s:%d: " msg "\n", __FILE__, __LINE__); abort();}

#define DEF_INIT_DLFCN_PTR(ftype, var, symbol) \
static ftype var = NULL; \
if (!var) var = (ftype)dlsym(RTLD_NEXT, symbol); \
DL_ERROR_IF(!var);

static const int _num_numa_nodes = CONF_NUM_NUMA_NODES;
static const int _num_cpus = CONF_NUM_CPUS;
static const int _cpu_to_node_mapping[] = {
  CONF_CPU_TO_NODE_MAPPING
};
static_assert(sizeof(_cpu_to_node_mapping) == _num_cpus * sizeof(_cpu_to_node_mapping[0]));
static int _node_to_real_node_mapping[_num_numa_nodes];

#define API_BEGIN() if (!_initialized) _initialize()

static bool _initialized = false;
static void _initialize() {
  int cpu_counts[_num_numa_nodes] = {0,};
  int physical_cpu_counts[_num_numa_nodes] = {0,};
  int cpu_to_real_node[_num_cpus] = {-1,};
  // Real numa function
  auto real_numa_node_of_cpu = (int(*)(int))dlsym(RTLD_NEXT, "numa_node_of_cpu");
  DL_ERROR_IF(!real_numa_node_of_cpu);
  // Verify config
  ALWAYS_ASSERT(_num_cpus == std::thread::hardware_concurrency(),
    "Number of cpus should be the actual number of cpus");
  for (int cpu = 0; cpu < _num_cpus; ++cpu) {
    const int node = _cpu_to_node_mapping[cpu];
    ++cpu_counts[node];
    // Check valid numa node
    ALWAYS_ASSERT(0 <= node && node < _num_numa_nodes,
      "Invalid numa node ID");
    // Check hyperthreaded sibling in the same node
    std::ifstream sibling_list_file(
      std::string("/sys/devices/system/cpu/cpu") +
      std::to_string(cpu) +
      "/topology/thread_siblings_list"
    );
    std::string sibling;
    int physical = -1;
    while (std::getline(sibling_list_file, sibling, ',')) {
      int sib = std::stoi(sibling);
      if (physical == -1) physical = (sib == cpu) ? 1 : 0;
      ALWAYS_ASSERT(_cpu_to_node_mapping[sib] == node,
        "Hyperthread siblings assigned to different numa nodes");
    }
    if (physical == 1) ++physical_cpu_counts[node];
    // Check real node
    cpu_to_real_node[cpu] = real_numa_node_of_cpu(cpu);
    ALWAYS_ASSERT(cpu_to_real_node[cpu] >= 0, "System error");
  }
  // Check equal number of cpus per node
  for (int node = 0; node < _num_numa_nodes; ++node) {
    ALWAYS_ASSERT(cpu_counts[node] == cpu_counts[0],
      "Unequal number of cpus per node");
    ALWAYS_ASSERT(physical_cpu_counts[node] == physical_cpu_counts[0],
      "Unequal number of physical cpus per node");
    _node_to_real_node_mapping[node] = -1;
  }
  // Check & assign real numa node per virtual numa node
  for (int cpu = 0; cpu < _num_cpus; ++cpu) {
    int virtual_node = _cpu_to_node_mapping[cpu];
    int real_node = cpu_to_real_node[cpu];
    if (_node_to_real_node_mapping[virtual_node] == -1) {
      // Assign virtual node -> real node mapping
      _node_to_real_node_mapping[virtual_node] = real_node;
    }
    else {
      // Check all cpus in a virtual node are actually in the same real node
      ALWAYS_ASSERT(_node_to_real_node_mapping[virtual_node] == real_node,
        "All cpus in a node should actually be in the real numa node");
    }
  }
  _initialized = true;
}

__API_FUNC__ int numa_available() {
  //printf("numa_available()\n");
  API_BEGIN();
  return 0;
}

__API_FUNC__ int numa_max_node() {
  //printf("numa_max_node()\n");
  API_BEGIN();
  return _num_numa_nodes - 1;
}

__API_FUNC__ int numa_node_of_cpu(int cpu) {
  //printf("numa_node_of_cpu(%d)\n", cpu);
  API_BEGIN();
  if (cpu < 0 || cpu >= _num_cpus) {
    errno = EINVAL;
    return -1;
  }
  return _cpu_to_node_mapping[cpu];
}

__API_FUNC__ void *numa_alloc_onnode(size_t size, int node) {
  //printf("numa_alloc_onnode(%lu, %d)\n", size, node);
  API_BEGIN();
  using _func_type = void*(*)(size_t,int);
  DEF_INIT_DLFCN_PTR(_func_type, real_func, "numa_alloc_onnode");
  return real_func(size, _node_to_real_node_mapping[node]);
}

__API_FUNC__ void numa_set_preferred(int node) {
  //printf("numa_set_preferred(%d)\n", node);
  API_BEGIN();
  using _func_type = void(*)(int);
  DEF_INIT_DLFCN_PTR(_func_type, real_func, "numa_set_preferred");
  real_func(_node_to_real_node_mapping[node]);
}

__API_FUNC__ int numa_run_on_node(int node) {
  //printf("numa_run_on_node(%d)\n", node);
  API_BEGIN();
  using _func_type = int(*)(int);
  DEF_INIT_DLFCN_PTR(_func_type, real_func, "numa_run_on_node");
  return real_func(_node_to_real_node_mapping[node]);
}
