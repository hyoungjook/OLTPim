#pragma once
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <linux/perf_event.h>
#include <linux/hw_breakpoint.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <vector>

namespace ermia::perf {

class InProcessPerf {
 public:
  struct event {
    struct format {
      uint64_t value;
      uint64_t time_enabled;
      uint64_t time_running;
      static uint64_t compute_diff(const format& prev, const format& curr) {
        return (uint64_t)(
          (double)(curr.value - prev.value) *
          (double)(curr.time_enabled - prev.time_enabled) /
          (double)(curr.time_running - prev.time_running)
        );
      }
    };
    int fd;
    format prev, data;
    void init(uint32_t type, uint64_t config, bool process_local) {
      struct perf_event_attr attr;
      memset(&attr, 0, sizeof(struct perf_event_attr));
      attr.type = type;
      attr.size = sizeof(struct perf_event_attr);
      attr.config = config;
      attr.sample_period = 0;
      attr.sample_type = PERF_SAMPLE_IDENTIFIER;
      attr.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
      attr.disabled = true;
      attr.inherit = 1;
      if (process_local) {
        fd = syscall(SYS_perf_event_open, &attr, 0, -1, -1, 0);
      }
      else {
        fd = syscall(SYS_perf_event_open, &attr, -1, 1, -1, 0);
      }
      if (fd < 0) {
        perror("perf_event_open");
        abort();
      }
    }
    void destroy() {
      close(fd);
    }
    void start() {
      int ret = ioctl(fd, PERF_EVENT_IOC_RESET);
      assert(ret != -1);
      ret = ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
      assert(ret != -1);
      ret = read(fd, &prev, sizeof(format));
      assert(ret == sizeof(format));
    }
    void stop() {
      int ret = ioctl(fd, PERF_EVENT_IOC_DISABLE, 0);
      assert(ret != -1);
    }
    uint64_t measure() {
      int ret = read(fd, &data, sizeof(format));
      assert(ret == sizeof(format));
      uint64_t result = format::compute_diff(prev, data);
      prev = data;
      return result;
    }
  };
  struct event_type {
    uint32_t type;
    uint64_t config;
    bool process_local;
  };

  InProcessPerf() {}
  ~InProcessPerf() {for (event &e: events) e.destroy();}
  void add_event(event_type type) {
    event_types.push_back(type);
    events.emplace_back();
    events.back().init(type.type, type.config, type.process_local);
  }
  void start() {for (event &e: events) e.start();}
  void stop() {for (event &e: events) e.stop();}
  uint64_t measure(event_type type) {
    for (size_t i = 0; i < event_types.size(); ++i) {
      if (type.type == event_types[i].type &&
          type.config == event_types[i].config &&
          type.process_local == event_types[i].process_local) {
        return events[i].measure();
      }
    }
    abort();
  }

 private:
  std::vector<event_type> event_types;
  std::vector<event> events;
};

void Initialize();

static const InProcessPerf::event_type PERF_EVENT_LLC_LOAD_MISSES =
  InProcessPerf::event_type{PERF_TYPE_HW_CACHE,
    (PERF_COUNT_HW_CACHE_LL) | (PERF_COUNT_HW_CACHE_OP_READ << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16),
    true
  };

static const InProcessPerf::event_type PERF_EVENT_LLC_STORE_MISSES =
  InProcessPerf::event_type{PERF_TYPE_HW_CACHE,
    (PERF_COUNT_HW_CACHE_LL) | (PERF_COUNT_HW_CACHE_OP_WRITE << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16),
    true
  };

// Power/energy-* varies among systems...
//static const InProcessPerf::event_type PERF_EVENT_POWER_ENERGY_PKG =
//  InProcessPerf::event_type{0x3f, 0x02, false};
//static const InProcessPerf::event_type PERF_EVENT_POWER_ENERGY_RAM =
//  InProcessPerf::event_type{0x3f, 0x03, false};
//static const double PERF_EVENT_POWER_SCALE = 2.3283064365386962890625e-10;

}