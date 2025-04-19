#pragma once

#include <stdint.h>
#include <sys/types.h>

#include <vector>

#include "dbcore/dlog.h"
#include "dbcore/dlog-tx.h"
#include "dbcore/xid.h"
#include "dbcore/sm-config.h"
#include "dbcore/sm-oid.h"
#include "dbcore/sm-object.h"
#include "dbcore/sm-rc.h"
#include "dbcore/sm-spinlock.h"
#include "masstree/masstree_btree.h"
#include "macros.h"
#include "str_arena.h"
#include "tuple.h"

#include <sparsehash/dense_hash_map>
using google::dense_hash_map;

#if defined(OLTPIM)
#include "dbcore/sm-coroutine.h"
#include "oltpim.h"

// Only supports YCSB-C!!
//#define OLTPIM_OFFLOAD_INDEX_ONLY

#endif

#if defined(OLTPIM)
#define COMMIT_SYNC(expr) sync_wait_oltpim_coro(expr)
#else
#define COMMIT_SYNC(expr) expr
#endif

namespace ermia {


#if defined(SSN) || defined(SSI)
#define set_tuple_xstamp(tuple, s)                                    \
  {                                                                   \
    uint64_t x;                                                       \
    do {                                                              \
      x = volatile_read(tuple->xstamp);                               \
    } while (x < s and                                                \
             not __sync_bool_compare_and_swap(&tuple->xstamp, x, s)); \
  }
#endif

// A write-set entry is essentially a pointer to the OID array entry
// begin updated. The write-set is naturally de-duplicated: repetitive
// updates will leave only one entry by the first update. Dereferencing
// the entry pointer results a fat_ptr to the new object.
struct write_record_t {
  fat_ptr *entry;
  bool is_insert;
  bool is_cold;
  FID fid;
  OID oid;
  uint64_t size;
  write_record_t(fat_ptr *entry, FID fid, OID oid, uint64_t size, bool insert, bool cold)
    : entry(entry), is_insert(insert), is_cold(cold), fid(fid), oid(oid), size(size) {}
  // Don't do anything on constructor here. It becomes bottleneck on large coro-batch-size.
  write_record_t() {}
  inline Object *get_object() { return (Object *)entry->offset(); }
};

struct write_set_t {
  static const uint32_t kMaxEntries = 256;
  uint32_t num_entries;
  write_record_t entries[kMaxEntries];
  write_set_t() : num_entries(0) {}
  inline void emplace_back(fat_ptr *oe, FID fid, OID oid, uint32_t size, bool insert, bool cold) {
    ALWAYS_ASSERT(num_entries < kMaxEntries);
    new (&entries[num_entries]) write_record_t(oe, fid, oid, size, insert, cold);
    ++num_entries;
    ASSERT(entries[num_entries - 1].entry == oe);
  }
  inline uint32_t size() { return num_entries; }
  inline void clear() { num_entries = 0; }
  inline write_record_t &operator[](uint32_t idx) { return entries[idx]; }
};

class transaction {
  friend class ConcurrentMasstreeIndex;
  friend struct sm_oid_mgr;

public:
  typedef TXN::txn_state txn_state;

#if defined(SSN) || defined(SSI) || defined(MVOCC)
  typedef std::vector<dbtuple *> read_set_t;
#endif

  enum {
    // use the low-level scan protocol for checking scan consistency,
    // instead of keeping track of absent ranges
    TXN_FLAG_LOW_LEVEL_SCAN = 0x1,

    // true to mark a read-only transaction- if a txn marked read-only
    // does a write, it is aborted. SSN uses it to implement to safesnap.
    // No bookeeping is done with SSN if this is enable for a tx.
    TXN_FLAG_READ_ONLY = 0x2,

    TXN_FLAG_READ_MOSTLY = 0x3,

    // A context-switch transaction doesn't enter/exit thread during construct/destruct.
    TXN_FLAG_CSWITCH = 0x8,
  };

  inline bool is_read_mostly() { return flags & TXN_FLAG_READ_MOSTLY; }
  inline bool is_read_only() { return flags & TXN_FLAG_READ_ONLY; }

protected:
  inline txn_state state() const { return xc->state; }

  // the absent set is a mapping from (masstree node -> version_number).
  typedef dense_hash_map<const ConcurrentMasstree::node_opaque_t *, uint64_t > MasstreeAbsentSet;
  MasstreeAbsentSet masstree_absent_set;

 public:
  transaction(uint64_t flags, str_arena &sa, uint32_t coro_batch_idx);
  ~transaction() {}

  void uninitialize();

#if defined OLTPIM
  ermia::coro::task<rc_t> oltpim_commit();
  ermia::coro::task<rc_t> oltpim_abort();
#endif

  rc_t commit();
#if defined SSN
  rc_t parallel_ssn_commit();
  rc_t ssn_read(dbtuple *tuple);
#elif defined SSI
  rc_t parallel_ssi_commit();
  rc_t ssi_read(dbtuple *tuple);
#elif defined MVOCC
  rc_t mvocc_commit();
  rc_t mvocc_read(dbtuple *tuple);
#else
  rc_t si_commit();
#endif

  bool MasstreeCheckPhantom();
  rc_t Abort();

  // Insert a record to the underlying table
  OID Insert(TableDescriptor *td, bool cold, varstr *value, dbtuple **out_tuple = nullptr);

  rc_t SyncUpdate(TableDescriptor *td, OID oid, const varstr *k, varstr *v);
  PROMISE(rc_t) Update(TableDescriptor *td, OID oid, const varstr *k, varstr *v);

  // Same as Update but without support for logging key
  inline rc_t SyncUpdate(TableDescriptor *td, OID oid, varstr *v) {
    auto rc = SyncUpdate(td, oid, nullptr, v);
    return rc;
  }
  inline PROMISE(rc_t) Update(TableDescriptor *td, OID oid, varstr *v) {
    auto rc = AWAIT Update(td, oid, nullptr, v);
    RETURN rc;
  }

  void LogIndexInsert(OrderedIndex *index, OID oid, const varstr *key);

public:
  // Reads the contents of tuple into v within this transaction context
  rc_t DoTupleRead(dbtuple *tuple, varstr *out_v);

  // expected public overrides

  inline str_arena &string_allocator() { return *sa; }

  inline void add_to_write_set(fat_ptr *entry, FID fid, OID oid, uint64_t size, bool insert, bool cold) {
#if defined(OLTPIM)
    ALWAYS_ASSERT(false);
#else
#ifndef NDEBUG
    for (uint32_t i = 0; i < write_set.size(); ++i) {
      auto &w = write_set[i];
      ASSERT(w.entry);
      ASSERT(w.entry != entry);
    }
#endif

    // Work out the encoded size to be added to the log block later
    auto logrec_size = align_up(size + sizeof(dbtuple) + sizeof(dlog::log_record));
    log_size += logrec_size;
    // Each write set entry still just records the size of the actual "data" to
    // be inserted to the log excluding dlog::log_record, which will be
    // prepended by log_insert/update etc.
    write_set.emplace_back(entry, fid, oid, size + sizeof(dbtuple), insert, cold);
#endif
  }

#if defined(OLTPIM)
  inline void add_to_pim_write_set(fat_ptr entry, uint32_t index_id, uint32_t pim_id, uint32_t oid, uint64_t size, bool insert) {
    // Work out the encoded size to be added to the log block later
    auto logrec_size = align_up(size + sizeof(dbtuple) + sizeof(dlog::log_record));
    log_size += logrec_size;
    // Each write set entry still just records the size of the actual "data" to
    // be inserted to the log excluding dlog::log_record, which will be
    // prepended by log_insert/update etc.
    pim_write_set.emplace_back(entry, index_id, pim_id, oid, size + sizeof(dbtuple), insert);
  }
  inline void add_to_pim_write_set_secondary_idx(uint32_t pim_id, uint32_t oid) {
    pim_write_set.emplace_back({(uint64_t)-1}, 0, pim_id, oid, 0, false);
  }
#endif

  inline TXN::xid_context *GetXIDContext() { return xc; }

  inline void set_cold(bool _is_disk) { is_disk = _is_disk; }
  inline bool is_cold() { return is_disk; }

  inline void set_abort_if_cold(bool _abort_if_cold) { m_abort_if_cold = _abort_if_cold; }
  inline bool abort_if_cold() { return m_abort_if_cold; }

  inline void set_forced_abort(bool _is_forced_abort) { m_is_forced_abort = _is_forced_abort; }
  inline bool is_forced_abort() { return m_is_forced_abort; }

  inline size_t get_expected_io_size() { return cold_log_io_size; }
  inline void set_expected_io_size(size_t _cold_log_io_size) {
    cold_log_io_size = _cold_log_io_size;
  }

  inline int *get_user_data() { return io_uring_user_data; }
  inline void set_user_data(int _user_data) {
    io_uring_user_data[0] = _user_data;
  }

  inline bool in_memory_queue() { return is_in_memory_queue; }
  inline void set_in_memory_queue(bool _is_in_memory_queue) {
    is_in_memory_queue = _is_in_memory_queue;
  }

  inline uint16_t index() { return pos_in_queue; }
  inline void set_index(uint16_t _pos_in_queue) {
    pos_in_queue = _pos_in_queue;
  }

  inline void set_position(bool _is_in_memory_queue, uint16_t _pos_in_queue) {
    is_in_memory_queue = _is_in_memory_queue;
    pos_in_queue = _pos_in_queue;
  }

  static uint64_t rdtsc(void) {
#if defined(__x86_64__)
    uint32_t low, high;
    asm volatile("rdtsc" : "=a"(low), "=d"(high));
    return (static_cast<uint64_t>(high) << 32) | static_cast<uint64_t>(low);
#else
#pragma message("Warning: unknown architecture, no rdtsc() support")
    return 0;
#endif
  }

 protected:
  const uint64_t flags;
  XID xid;
  TXN::xid_context *xc;
  dlog::tls_log *log;
  uint32_t log_size;
  str_arena *sa;
  uint32_t coro_batch_idx; // its index in the batch
#if defined(SSN) || defined(SSI) || defined(MVOCC)
  read_set_t read_set;
#endif
  bool is_disk;
  bool is_in_memory_queue;
  bool m_abort_if_cold;
  bool m_is_forced_abort;
  uint16_t pos_in_queue;
  size_t cold_log_io_size;
  int io_uring_user_data[1];
#if defined(OLTPIM)
  ermia::pim::write_set_t pim_write_set;
  // pim_request_buffer should be the last member
  uint8_t pim_request_buffer[
    sizeof(oltpim::request_finalize) * ermia::pim::write_set_t::kMaxEntries];
  static_assert(sizeof(oltpim::request_finalize) == sizeof(oltpim::request_finalize_ws));
#else
  write_set_t write_set;
#endif
};

}  // namespace ermia
