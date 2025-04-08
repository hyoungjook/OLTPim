#!/bin/bash

# After compiling, use LD_PRELOAD=/mosaicdb/customnuma/libnuma.so
# to inject custom numa API to the program.

g++ -std=c++17 -shared -fPIC -O3 -g -o libnuma.so numa.cpp -ldl
