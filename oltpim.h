#pragma once
#if defined(OLTPIM)
#include "dbcore/sm-common.h"
#include "dbcore/sm-coroutine.h"
#include "interface_host.hpp"

namespace ermia {

class ConcurrentMasstreeIndex;

namespace pim {

// call this after oltpim::engine::g_engine.add_index()
void register_index(ConcurrentMasstreeIndex *index);
// call this after adding all indexes
void finalize_index_setup();
// call this for all existing indexes
void set_index_partition_interval(
  const char *index_name, uint64_t interval_bits, bool numa_local, uint32_t numa_id);

struct log_record_t {
  // Normal record (insert, update): all fields are valid (MSB of oid is 0)
  // Tombstone record: entry = NULL_PTR, size = 0, all other fields are valid (MSB of oid is 0)
  // Secondary index record: entry = -1, only pim_id & oid are valid. Not logged. (MSB of oid is 1)
  fat_ptr entry;
  bool is_insert: 1;
  bool is_pim_index: 1;
  uint8_t index_id;
  uint16_t pim_id;
  uint32_t oid;
  uint64_t size;
  // Don't do anything on constructor here. It becomes bottleneck on large coro-batch-size.
  log_record_t() {}
  log_record_t(fat_ptr entry, uint8_t index_id, uint16_t pim_id, uint32_t oid, uint64_t size, bool insert, bool is_pim_index)
    : entry(entry), is_insert(insert), is_pim_index(is_pim_index), index_id(index_id), pim_id(pim_id), oid(oid), size(size) {}
  inline Object *get_object() {
    if (is_pim_index) return (Object*)entry.offset();
    else return (Object*)((fat_ptr*)entry._ptr)->offset();
  }
  inline bool is_secondary_index_record() {
    if (is_pim_index) return (entry._ptr == (uint64_t)-1);
    else return false;
  }
};

struct write_set_t {
  static const uint32_t kMaxEntries = 256;
  uint32_t num_entries;
  log_record_t entries[kMaxEntries];
  write_set_t() : num_entries(0) {}
  inline void emplace_back(fat_ptr e, uint32_t iid, uint32_t pid, uint32_t oid, uint64_t size, bool insert, bool pim) {
    ALWAYS_ASSERT(num_entries < kMaxEntries);
    new (&entries[num_entries]) log_record_t(e, iid, pid, oid, size, insert, pim);
    ++num_entries;
  }
  inline uint32_t size() {return num_entries;}
  inline void clear() {num_entries = 0;}
  inline log_record_t &operator[](uint32_t idx) {return entries[idx];}
  // destroys entries[] and num_entries, so should be clear()ed after calling.
  // extract unique pim_ids in entries[] and store it adjacently in the same place.
  inline uint16_t *pim_id_sort() {
    uint16_t i, j;
    uint16_t *pim_ids = (uint16_t*)entries;
    // compress, loop from begin to end; only for valid pim_id
    j = 0;
    for (i = 0; i < num_entries; ++i) {
      if (entries[i].is_pim_index) {
        pim_ids[j] = entries[i].pim_id;
        ++j;
      }
    }
    num_entries = j;
    // in-place sort
    std::sort(pim_ids, pim_ids + num_entries);
    return pim_ids;
  }
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
  int storeval; // used to store cnt on pim_ScanBegin/End()
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
