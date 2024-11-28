#pragma once
#if defined(OLTPIM)
#include "dbcore/sm-common.h"
#include "dbcore/sm-coroutine.h"

namespace ermia {

class ConcurrentMasstreeIndex;

namespace pim {

// call this after oltpim::engine::g_engine.add_index()
void register_index(ConcurrentMasstreeIndex *index);
// call this after adding all indexes
void finalize_index_setup();
// call this for all existing indexes
void set_index_partition_interval(
  const char *index_name, uint64_t pim_bits, uint64_t numa_bits);

struct log_record_t {
  fat_ptr entry;
  uint32_t index_id;
  uint32_t pim_id;
  uint32_t oid;
  uint64_t size;
  bool is_insert;
  // Don't do anything on constructor here. It becomes bottleneck on large coro-batch-size.
  log_record_t() {}
  log_record_t(fat_ptr entry, uint32_t index_id, uint32_t pim_id, uint32_t oid, uint64_t size, bool insert)
    : entry(entry), index_id(index_id), pim_id(pim_id), oid(oid), size(size), is_insert(insert) {}
  inline Object *get_object() {return (Object*)entry.offset();}
};

struct write_set_t {
  static const uint32_t kMaxEntries = 256;
  uint32_t num_entries;
  log_record_t entries[kMaxEntries];
  write_set_t() : num_entries(0) {}
  inline void emplace_back(fat_ptr e, uint32_t iid, uint32_t pid, uint32_t oid, uint64_t size, bool insert) {
    ALWAYS_ASSERT(num_entries < kMaxEntries);
    new (&entries[num_entries]) log_record_t(e, iid, pid, oid, size, insert);
    ++num_entries;
  }
  inline uint32_t size() {return num_entries;}
  inline void clear() {num_entries = 0;}
  inline log_record_t &operator[](uint32_t idx) {return entries[idx];}
};

class PIMScanCallback {
public:
  virtual bool Invoke(const varstr &value) = 0;
  virtual uint32_t max_outs_per_interval() = 0;
  virtual uint32_t num_intervals() = 0;
  // Child class provides the storage
  virtual void *scan_req_storage() = 0; // request_scan<max_outs_per_interval>[num_intervals]
  virtual size_t scan_req_storage_size() = 0; // sizeof(request_scan<max_outs_per_interval>)
  virtual void *get_req_storage() = 0;  // request_get[max_outs]
};

}

}

template <typename T>
inline T sync_wait_oltpim_coro(ermia::coro::task<T> &&coro_task) {
  coro_task.start();
  while (!coro_task.done()) {
    coro_task.resume();
  }
  return coro_task.get_return_value();
}

#endif // defined(OLTPIM)
