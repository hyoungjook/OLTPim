#pragma once

/*#define CONF_NUM_NUMA_NODES 2
#define CONF_NUM_CPUS 64
#define CONF_CPU_TO_NODE_MAPPING \
0, 0, 0, 0, 0, 0, 0, 0, \
0, 0, 0, 0, 0, 0, 0, 0, \
1, 1, 1, 1, 1, 1, 1, 1, \
1, 1, 1, 1, 1, 1, 1, 1, \
0, 0, 0, 0, 0, 0, 0, 0, \
0, 0, 0, 0, 0, 0, 0, 0, \
1, 1, 1, 1, 1, 1, 1, 1, \
1, 1, 1, 1, 1, 1, 1, 1,
#define CONF_NUM_PIMRANKS 32
#define CONF_PIMRANK_TO_NODE_MAPPING \
0, 0, 0, 0, 0, 0, 0, 0, \
0, 0, 0, 0, 0, 0, 0, 0, \
1, 1, 1, 1, 1, 1, 1, 1, \
1, 1, 1, 1, 1, 1, 1, 1,*/

#define CONF_NUM_NUMA_NODES 4
#define CONF_NUM_CPUS 64
#define CONF_CPU_TO_NODE_MAPPING \
0, 0, 0, 0, 0, 0, 0, 0, \
1, 1, 1, 1, 1, 1, 1, 1, \
2, 2, 2, 2, 2, 2, 2, 2, \
3, 3, 3, 3, 3, 3, 3, 3, \
0, 0, 0, 0, 0, 0, 0, 0, \
1, 1, 1, 1, 1, 1, 1, 1, \
2, 2, 2, 2, 2, 2, 2, 2, \
3, 3, 3, 3, 3, 3, 3, 3,
#define CONF_NUM_PIMRANKS 32
#define CONF_PIMRANK_TO_NODE_MAPPING \
0, 0, 0, 0, 0, 0, 0, 0, \
1, 1, 1, 1, 1, 1, 1, 1, \
2, 2, 2, 2, 2, 2, 2, 2, \
3, 3, 3, 3, 3, 3, 3, 3,
