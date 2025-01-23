#!/bin/bash

# Embedded packages
sudo apt-get update -y
sudo apt-get install -y cmake curl gcc g++ git gnupg libnuma1 make \
    openjdk-11-jdk pkg-config python-dev-is-python3 python3-pip python3-serial \
    vim wget
pip3 install pyyaml 'pyelftools>=0.26'

# MosaicDB specific files
sudo apt-get install -y build-essential python2 git vim gdb numactl pkg-config \
    cmake gcc-10 g++-10 libc++-dev libc++abi-dev libnuma-dev libibverbs-dev libgflags-dev liburing-dev libudev-dev \
    python3 python3-pip
pip3 install pyelftools psutil

cd $HOME && git clone https://github.com/google/glog.git && \
    cd $HOME/glog && git checkout v0.5.0 && \
    cmake -S . -B build -G "Unix Makefiles" && \
    cmake --build build && \
    cmake --build build --target install && \
    cd $HOME && rm -rf glog
