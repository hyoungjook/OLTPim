#if defined(OLTPIM)
#include <mutex>
#include <random>
#include <gflags/gflags.h>
#include "engine.h"
#include "txn.h"
#include "sm-table.h"
#include "sm-alloc.h"
#include "engine.hpp"
#include "interface_host.hpp"

DEFINE_uint32(oltpim_num_ranks_per_numa_node, 1, "Number of PIM ranks to allocate per numa node");
DEFINE_bool(oltpim_interleave, true, "Enable CPU-PIM interleaved execution");

namespace ermia {

namespace pim {

static std::vector<ConcurrentMasstreeIndex*> registered_indexes;
static std::mt19937 rand_gen(7777);

void register_index(ConcurrentMasstreeIndex *index) {
  registered_indexes.push_back(index);
}

void finalize_index_setup() {
  oltpim::engine::config config = {
    .num_ranks_per_numa_node = (int)FLAGS_oltpim_num_ranks_per_numa_node,
    .alloc_fn = ermia::config::tls_alloc ? ermia::MM::allocate_onnode : nullptr,
#if !defined(OLTPIM_OFFLOAD_INDEX_ONLY)
    .gc_prob = (ermia::config::enable_gc ? ermia::config::gc_prob : 0),
#else
    .gc_prob = 0,
#endif
    .enable_measure_energy = (ermia::config::measure_energy != 0),
    .enable_interleave = FLAGS_oltpim_interleave
  };
  oltpim::engine::g_engine.init(config);
  uint32_t num_pims = oltpim::engine::g_engine.num_pims();
  uint32_t num_pims_per_numa = oltpim::engine::g_engine.num_pims_per_numa_node();
  uint32_t num_numas = num_pims / num_pims_per_numa;
  std::uniform_int_distribution<uint32_t> rand_offset(0, num_pims - 1);
  for (auto *index: registered_indexes) {
    index->set_num_pims(num_numas, num_pims_per_numa, rand_offset(rand_gen));
  }
}

void set_index_partition_interval(
    const char *index_name, uint64_t interval_bits, bool numa_local, uint32_t numa_id) {
  ((ermia::ConcurrentMasstreeIndex*)
    ermia::TableDescriptor::GetIndex(index_name))->set_key_interval(
      interval_bits, numa_local, numa_id);
}

} // namespace pim

static int g_concurrent_masstree_index_id = 0;
static std::mutex g_concurrent_masstree_index_id_assign_mutex;

void ConcurrentMasstreeIndex::assign_index_id() {
  std::unique_lock lck(g_concurrent_masstree_index_id_assign_mutex);
  index_id = g_concurrent_masstree_index_id;
  ++g_concurrent_masstree_index_id;
}

static void
pim_gc_tuple_chain(uint64_t gc_begin, uint16_t gc_num) {
  fat_ptr obj_ptr{gc_begin};
  for (uint16_t i = 0; i < gc_num; ++i) {
    auto *obj = (Object*)obj_ptr.offset();
    fat_ptr next_ptr = obj->GetNextPersistent();
    MM::deallocate(obj_ptr);
    obj_ptr = next_ptr;
  }
}

#if !defined(OLTPIM_OFFLOAD_INDEX_ONLY)
void
ConcurrentMasstreeIndex::pim_GetRecordBegin(transaction *t, const uint64_t key, void *req_) {
  ALWAYS_ASSERT(IsPrimary());
  auto *xc = t->xc;
  auto *req = (oltpim::request_get*)req_;
  auto &args = req->args;
  args.key = key;
  args.xid_s.xid = (xc->owner._val) >> 16;
  args.xid_s.index_id = index_id;
  args.xid_s.oid_query = 0;
  args.csn = xc->begin;
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, req);
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_GetRecordEnd(transaction *t, varstr &value, void *req_) {
  auto *req = (oltpim::request_get*)req_;
  while (!oltpim::engine::g_engine.is_done(req)) {
    co_await std::suspend_always{};
  }
  auto &rets = req->rets;
  auto status = REQ_GET_STATUS(rets.value_status);
  CHECK_VALID_STATUS(status);
  rc_t rc;
  if (status != STATUS_SUCCESS) {
    rc = (status == STATUS_FAILED) ? rc_t{RC_FALSE} : rc_t{RC_ABORT_SI_CONFLICT};
    co_return rc;
  }
  fat_ptr obj = {rets.value_status};
  auto *tuple = (dbtuple*)((Object*)obj.offset())->GetPayload();
  value.p = tuple->get_value_start();
  value.l = tuple->size;
  co_return rc_t{RC_TRUE};
}
#endif

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_GetRecord(transaction *t, const uint64_t key, varstr &value) {
#if defined(OLTPIM_OFFLOAD_INDEX_ONLY)
  ALWAYS_ASSERT(false); // Not supported
#endif
  auto *xc = t->xc;
  rc_t rc;

  oltpim::request_get req;
  auto &args = req.args;
  args.key = key;
  args.xid_s.xid = (xc->owner._val) >> 16;
  args.xid_s.index_id = index_id;
  args.xid_s.oid_query = 0;
  args.csn = xc->begin;
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, &req);
  while (!oltpim::engine::g_engine.is_done(&req)) {
    co_await std::suspend_always{};
  }
  auto &rets = req.rets;

  if (!IsPrimary()) { // Secondary
    auto status = REQ_GET_STATUS(rets.value_status);
    if (status != STATUS_SUCCESS) {
      rc = (status == STATUS_FAILED) ? rc_t{RC_FALSE} : rc_t{RC_ABORT_SI_CONFLICT};
      co_return rc;
    }
    // Query again with the same req struct
    pim_id = (int)SVALUE_GET_PIMID(rets.value_status);
    uint32_t local_oid = SVALUE_GET_OID(rets.value_status);
    args.key = (uint64_t)local_oid;
    args.xid_s.xid = (xc->owner._val) >> 16;
    args.xid_s.index_id = ((ConcurrentMasstreeIndex*)table_descriptor->GetPrimaryIndex())->index_id;
    args.xid_s.oid_query = 1;
    args.csn = xc->begin;
    // reuse req; already points to correct args and rets
    oltpim::engine::g_engine.push(pim_id, &req);
    while (!oltpim::engine::g_engine.is_done(&req)) {
      co_await std::suspend_always{};
    }
  }

  auto status = REQ_GET_STATUS(rets.value_status);
  CHECK_VALID_STATUS(status);
  if (status != STATUS_SUCCESS) {
    rc = (status == STATUS_FAILED) ? rc_t{RC_FALSE} : rc_t{RC_ABORT_SI_CONFLICT};
    co_return rc;
  }
  fat_ptr obj = {rets.value_status};
  auto *tuple = (dbtuple*)((Object*)obj.offset())->GetPayload();
  value.p = tuple->get_value_start();
  value.l = tuple->size;
  rc = tuple->size > 0 ? rc_t{RC_TRUE} : rc_t{RC_FALSE};
  co_return rc;
}

#if !defined(OLTPIM_OFFLOAD_INDEX_ONLY)
void
ConcurrentMasstreeIndex::pim_InsertRecordBegin(transaction *t, const uint64_t key, varstr &value, void *req_) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());
  auto *xc = t->xc;
  fat_ptr new_obj = Object::Create(&value, xc->begin_epoch);
  auto *req = (oltpim::request_insert*)req_;
  auto &args = req->args;
  args.key = key;
  args.value = new_obj._ptr;
  args.xid_s.xid = (xc->owner._val) >> 16;
  args.xid_s.index_id = index_id;
  args.csn = xc->begin;
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, req);
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_InsertRecordEnd(transaction *t, void *req_, uint64_t *oid) {
  auto *req = (oltpim::request_insert*)req_;
  while (!oltpim::engine::g_engine.is_done(req)) {
    co_await std::suspend_always{};
  }
  // recover new_obj from req->args->value
  fat_ptr new_obj = {req->args.value};
  auto &rets = req->rets;
  CHECK_VALID_STATUS(rets.status);
  if (rets.status != STATUS_SUCCESS) {
    MM::deallocate(new_obj);
    uint16_t rc = (rets.status == STATUS_FAILED) ? RC_FALSE : RC_ABORT_SI_CONFLICT;
    co_return {rc};
  }
  dbtuple *tuple = (dbtuple*)((Object*)new_obj.offset())->GetPayload();
  int pim_id = pim_id_of(req->args.key);
  t->add_to_pim_write_set(new_obj, index_id, pim_id, rets.oid, tuple->size, true);
  if (ermia::config::enable_gc) pim_gc_tuple_chain(rets.gc_begin, rets.gc_num);
  if (oid) *oid = SVALUE_MAKE(pim_id, rets.oid);
  co_return {RC_TRUE};
}
#endif

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_InsertRecord(transaction *t, const uint64_t key, varstr &value, uint64_t *oid) {
  oltpim::request_insert req;
  pim_InsertRecordBegin(t, key, value, &req);
  auto rc = co_await pim_InsertRecordEnd(t, &req, oid);
  co_return rc;
}

void
ConcurrentMasstreeIndex::pim_InsertOIDBegin(transaction *t, const uint64_t key, uint64_t oid, void *req_) {
  // For secondary index only
  ALWAYS_ASSERT(!IsPrimary());
  auto *xc = t->xc;
  auto *req = (oltpim::request_insert*)req_;
  auto &args = req->args;
  args.key = key;
  args.value = oid;
  args.xid_s.xid = (xc->owner._val) >> 16;
  args.xid_s.index_id = index_id;
  args.csn = xc->begin;
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, req);
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_InsertOIDEnd(transaction *t, void *req_) {
  auto *req = (oltpim::request_insert*)req_;
  while (!oltpim::engine::g_engine.is_done(req)) {
    co_await std::suspend_always{};
  }
  auto &rets = req->rets;
  CHECK_VALID_STATUS(rets.status);
  if (rets.status != STATUS_SUCCESS) {
    uint16_t rc = (rets.status == STATUS_FAILED) ? RC_FALSE : RC_ABORT_SI_CONFLICT;
    co_return {rc};
  }
  t->add_to_pim_write_set_secondary_idx(pim_id_of(req->args.key));
  co_return {RC_TRUE};
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_InsertOID(transaction *t, const uint64_t key, uint64_t oid) {
  oltpim::request_insert req;
  pim_InsertOIDBegin(t, key, oid, &req);
  auto rc = co_await pim_InsertOIDEnd(t, &req);
  co_return rc;
}

#if !defined(OLTPIM_OFFLOAD_INDEX_ONLY)
void
ConcurrentMasstreeIndex::pim_UpdateRecordBegin(transaction *t, const uint64_t key, varstr &value, void *req_) {
  ALWAYS_ASSERT(IsPrimary());
  auto *xc = t->xc;
  fat_ptr new_obj = Object::Create(&value, xc->begin_epoch);
  auto *req = (oltpim::request_update*)req_;
  auto &args = req->args;
  args.key = key;
  args.new_value = new_obj._ptr;
  args.xid_s.xid = (xc->owner._val) >> 16;
  args.xid_s.index_id = index_id;
  args.csn = xc->begin;
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, req);
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_UpdateRecordEnd(transaction *t, void *req_) {
  auto *req = (oltpim::request_update*)req_;
  while (!oltpim::engine::g_engine.is_done(req)) {
    co_await std::suspend_always{};
  }
  auto &rets = req->rets;
  auto status = rets.status;
  CHECK_VALID_STATUS(status);
  // Recover new_obj
  fat_ptr new_obj{req->args.new_value};
  if (status != STATUS_SUCCESS) {
    MM::deallocate(new_obj);
    rc_t rc = (status == STATUS_FAILED) ? rc_t{RC_FALSE} : rc_t{RC_ABORT_SI_CONFLICT};
    co_return rc;
  }
  auto *obj = (Object*)new_obj.offset();
  obj->SetNextPersistent(fat_ptr{rets.old_value}); // connect tuple chain
  auto *tuple = (dbtuple*)obj->GetPayload();
  int pim_id = pim_id_of(req->args.key);
  t->add_to_pim_write_set(new_obj, index_id, pim_id, rets.oid, tuple->size, false);
  if (ermia::config::enable_gc) pim_gc_tuple_chain(rets.gc_begin, rets.gc_num);
  co_return {RC_TRUE};
}
#endif

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_UpdateRecord(transaction *t, const uint64_t key, varstr &value) {
  oltpim::request_update req;
  pim_UpdateRecordBegin(t, key, value, &req);
  auto rc = co_await pim_UpdateRecordEnd(t, &req);
  co_return rc;
}

void
ConcurrentMasstreeIndex::pim_RemoveRecordBegin(transaction *t, const uint64_t key, void *req_) {
  ALWAYS_ASSERT(IsPrimary());
  auto *xc = t->xc;
  auto *req = (oltpim::request_remove*)req_;
  auto &args = req->args;
  args.key = key;
  args.xid_s.xid = (xc->owner._val) >> 16;
  args.xid_s.index_id = index_id;
  args.csn = xc->begin;
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, req);
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_RemoveRecordEnd(transaction *t, void *req_) {
  auto *req = (oltpim::request_remove*)req_;
  while (!oltpim::engine::g_engine.is_done(req)) {
    co_await std::suspend_always{};
  }
  auto &rets = req->rets;
  auto status = rets.status;
  CHECK_VALID_STATUS(status);
  if (status != STATUS_SUCCESS) {
    rc_t rc = (status == STATUS_FAILED) ? rc_t{RC_FALSE} : rc_t{RC_ABORT_SI_CONFLICT};
    co_return rc;
  }
  int pim_id = pim_id_of(req->args.key);
  t->add_to_pim_write_set(NULL_PTR, index_id, pim_id, rets.oid, 0, false);
  if (ermia::config::enable_gc) pim_gc_tuple_chain(rets.gc_begin, rets.gc_num);
  co_return {RC_TRUE};
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_RemoveRecord(transaction *t, const uint64_t key) {
  oltpim::request_remove req;
  pim_RemoveRecordBegin(t, key, &req);
  auto rc = co_await pim_RemoveRecordEnd(t, &req);
  co_return rc;
}

void
ConcurrentMasstreeIndex::pim_ScanBegin(transaction *t, const uint64_t start_key, const uint64_t end_key,
              pim::PIMScanCallback &callback, uint32_t max_keys_per_interval) {
  auto *xc = t->xc;
  rc_t rc;
  ASSERT(max_keys_per_interval <= callback.max_outs_per_interval() * callback.num_intervals());
  ASSERT(start_key <= end_key);
  const uint64_t xid = (xc->owner._val) >> 16;
  const uint64_t csn = xc->begin;

  using request_scan_base = typename oltpim::request_scan<0>::t;
  uint8_t *scan_req = (uint8_t*)callback.scan_req_storage();
  const size_t scan_req_size = callback.scan_req_storage_size(); 
  const int pim_id_end = pim_id_of(end_key);
  int cnt = 0;
  const uint64_t key_interval = (1UL << key_interval_bits);
  for (
      uint64_t begin_key = start_key & (~(key_interval - 1));
      begin_key <= end_key; begin_key += key_interval, ++cnt) {
    auto *req = (request_scan_base*)&scan_req[scan_req_size * cnt];
    auto &args = req->args;
    args.xid_s.max_outs = max_keys_per_interval;
    args.xid_s.index_id = index_id;
    args.xid_s.xid = xid;
    args.keys[0] = std::max(begin_key, start_key);
    args.keys[1] = std::min(begin_key + key_interval - 1, end_key);
    args.csn = csn;
    oltpim::engine::g_engine.push(pim_id_of(begin_key), req);
  }
  ASSERT(cnt <= callback.num_intervals());
  callback.storeval = cnt;
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_ScanEnd(transaction *t, pim::PIMScanCallback &callback,
    uint32_t max_key_per_interval) {
  uint8_t *scan_req = (uint8_t*)callback.scan_req_storage();
  const size_t scan_req_size = callback.scan_req_storage_size();
  int cnt = callback.storeval;
  using request_scan_base = typename oltpim::request_scan<0>::t;
  for (int i = 0; i < cnt; ++i) {
    auto *req = (request_scan_base*)&scan_req[scan_req_size * i];
    while (!oltpim::engine::g_engine.is_done(req)) {
      co_await std::suspend_always{};
    }
  }
  bool all_failed = true;
  for (int i = 0; i < cnt; ++i) {
    auto *req = (request_scan_base*)&scan_req[scan_req_size * i];
    auto status = req->rets.base.status;
    CHECK_VALID_STATUS(status);
    if (status == STATUS_SUCCESS) all_failed = false;
    else if (status != STATUS_FAILED) co_return {RC_ABORT_SI_CONFLICT};
  }
  if (all_failed) co_return {RC_FALSE};

  if (!IsPrimary()) { // Secondary: query again
    auto *xc = t->xc;
    const uint64_t xid = (xc->owner._val) >> 16;
    const uint64_t csn = xc->begin;
    const uint8_t primary_index_id = ((ConcurrentMasstreeIndex*)table_descriptor->GetPrimaryIndex())->index_id;
    auto *get_reqs = (oltpim::request_get*)callback.get_req_storage();
    int cnt2 = 0;
    for (int i = 0; i < cnt; ++i) {
      auto *req = (request_scan_base*)&scan_req[scan_req_size * i];
      auto &rets = req->rets;
      for (uint32_t j = 0; j < rets.base.outs; ++j) {
        const uint64_t ret_value = rets.values[j];
        auto &args = get_reqs[cnt2].args;
        args.key = (uint64_t)SVALUE_GET_OID(ret_value);
        args.xid_s.xid = xid;
        args.xid_s.index_id = primary_index_id;
        args.xid_s.oid_query = 1;
        args.csn = csn;
        oltpim::engine::g_engine.push((int)SVALUE_GET_PIMID(ret_value), &get_reqs[cnt2]);
        ++cnt2;
      }
    }
    ASSERT(cnt2 <= max_key_per_interval * cnt);
    callback.storeval = cnt2;
  }
  else { // Primary: ends here
    for (int i = 0; i < cnt; ++i) {
      auto *req = (request_scan_base*)&scan_req[scan_req_size * i];
      auto &rets = req->rets;
      for (uint32_t j = 0; j < rets.base.outs; ++j) {
        fat_ptr obj = {rets.values[j]};
        auto *tuple = (dbtuple*)((Object*)obj.offset())->GetPayload();
        varstr value(tuple->get_value_start(), tuple->size);
        if (!callback.Invoke(value)) break;
      }
    }
  }
  co_return {RC_TRUE};
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_ScanEndSecondary(transaction *t, pim::PIMScanCallback &callback) {
  ALWAYS_ASSERT(!IsPrimary());
  int cnt2 = callback.storeval;
  auto *get_reqs = (oltpim::request_get*)callback.get_req_storage();
  for (int i = 0; i < cnt2; ++i) {
    while (!oltpim::engine::g_engine.is_done(&get_reqs[i])) {
      co_await std::suspend_always{};
    }
  }
  for (int i = 0; i < cnt2; ++i) {
    auto status = REQ_GET_STATUS(get_reqs[i].rets.value_status);
    CHECK_VALID_STATUS(status);
    if (status != STATUS_SUCCESS) {
      uint16_t rc = (status == STATUS_FAILED) ? RC_FALSE : RC_ABORT_SI_CONFLICT;
      co_return {rc};
    }
  }
  for (int i = 0; i < cnt2; ++i) {
    fat_ptr obj = {get_reqs[i].rets.value_status};
    auto *tuple = (dbtuple*)((Object*)obj.offset())->GetPayload();
    varstr value(tuple->get_value_start(), tuple->size);
    if (!callback.Invoke(value)) break;
  }
  co_return {RC_TRUE};
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_Scan(transaction *t, const uint64_t start_key, const uint64_t end_key,
                              pim::PIMScanCallback &callback, uint32_t max_keys_per_interval) {
  pim_ScanBegin(t, start_key, end_key, callback, max_keys_per_interval);
  auto rc = co_await pim_ScanEnd(t, callback, max_keys_per_interval);
  if (rc._val != RC_TRUE) co_return rc;
  if (!IsPrimary()) {
    rc = co_await pim_ScanEndSecondary(t, callback);
  }
  co_return rc;
}

#if defined(OLTPIM_OFFLOAD_INDEX_ONLY)
void
ConcurrentMasstreeIndex::pim_GetRecordBegin(transaction *t, const uint64_t key, void *req_) {
  ALWAYS_ASSERT(IsPrimary());
  auto *req = (oltpim::request_getonly*)req_;
  // req_ actually points to oltpim::request_get, just use the storage as
  // oltpim::request_getonly.
  new (req) oltpim::request_getonly;
  auto &args = req->args;
  args.key = key;
  args.index_id = index_id;
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, req);
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_GetRecordEnd(transaction *t, varstr &value, void *req_) {
  auto *req = (oltpim::request_getonly*)req_;
  while (!oltpim::engine::g_engine.is_done(req)) {
    co_await std::suspend_always{};
  }
  auto &rets = req->rets;
  bool found = (rets.status == STATUS_SUCCESS);
  dbtuple *tuple = nullptr;
  if (found) {
    OID oid = rets.value;
    {
      // tuple = oidmgr->oid_get_version(table_descriptor->GetTupleArray(), oid, t->xc);
      fat_ptr *entry = table_descriptor->GetTupleArray()->get(oid);
start_over:
      fat_ptr ptr = volatile_read(*entry);
      while (ptr.offset()) {
        Object *cur_obj = (Object*)ptr.offset();
        fat_ptr tentative_next = cur_obj->GetNextVolatile();
        bool retry = false;
        bool visible = oidmgr->TestVisibility(cur_obj, t->xc, retry);
        if (retry) {
          co_await std::suspend_always{};
          goto start_over;
        }
        if (visible) {
          tuple = (dbtuple*)cur_obj->GetPinnedTuple(t);
          goto end;
        }
        ptr = tentative_next;
      }
      tuple = nullptr;
    }
end:
    if (!tuple) found = false;
  }
  rc_t rc{RC_FALSE};
  if (found) {
    rc = t->DoTupleRead(tuple, &value);
  }
  co_return rc;
}

void
ConcurrentMasstreeIndex::pim_InsertRecordBegin(transaction *t, const uint64_t key, varstr &value, void *req_) {
  ALWAYS_ASSERT(IsPrimary());
  // t->Insert(table_descriptor, false, &value, &tuple);
  OID oid;
  {
    auto *tuple_array = table_descriptor->GetTupleArray();
    FID tuple_fid = table_descriptor->GetTupleFid();
    fat_ptr new_head = Object::Create(&value, t->xc->begin_epoch);
    auto *tuple = (dbtuple*)((Object*)new_head.offset())->GetPayload();
    tuple->GetObject()->SetCSN(t->xid.to_ptr());
    oid = oidmgr->alloc_oid(tuple_fid);
    ALWAYS_ASSERT(oid != INVALID_OID);
    oidmgr->oid_put_new(tuple_array, oid, new_head);
    t->add_to_pim_write_set(
      fat_ptr{(uint64_t)tuple_array->get(oid)}, // indexonly: store &fat_ptr
      index_id, tuple_fid, oid, tuple->size, true);
  }
  // Insert to the index
  auto *req = (oltpim::request_insertonly*)req_;
  // req_ actually points to oltpim::request_insert, just use the storage as
  // oltpim::request_insertonly.
  new (req) oltpim::request_insertonly;
  auto &args = req->args;
  args.key = key;
  args.value = oid;
  args.index_id = index_id;
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, req);
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_InsertRecordEnd(transaction *t, void *req_, uint64_t *oid) {
  auto *req = (oltpim::request_insertonly*)req_;
  while (!oltpim::engine::g_engine.is_done(req)) {
    co_await std::suspend_always{};
  }
  auto &rets = req->rets;
  if (rets.status != STATUS_SUCCESS) {
    co_return {RC_FALSE};
  }
  if (oid) *oid = req->args.value;
  co_return {RC_TRUE};
}

void
ConcurrentMasstreeIndex::pim_UpdateRecordBegin(transaction *t, const uint64_t key, varstr &value, void *req_) {
  ALWAYS_ASSERT(IsPrimary());
  auto *req = (oltpim::request_getonly*)req_;
  // req_ actually points to oltpim::request_update, just use the storage as
  // oltpim::request_getonly.
  new (req) oltpim::request_getonly;
  auto &args = req->args;
  args.key = key;
  args.index_id = index_id;
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, req);
  // store &value after req
  *(varstr**)(req + 1) = &value;
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_UpdateRecordEnd(transaction *t, void *req_) {
  auto *req = (oltpim::request_getonly*)req_;
  while (!oltpim::engine::g_engine.is_done(req)) {
    co_await std::suspend_always{};
  }
  auto &rets = req->rets;
  bool found = (rets.status == STATUS_SUCCESS);

  if (found) {
    // t->Update(table_descriptor, oid, key=req->args.key, value=v)
    OID oid = rets.value;
    varstr *v = *(varstr**)(req + 1);
    fat_ptr new_obj_ptr = NULL_PTR;
    fat_ptr prev_obj_ptr = NULL_PTR;
    auto *oa = table_descriptor->GetTupleArray();
    {
      //fat_ptr prev_obj_ptr = oidmgr->UpdateTuple(
      //  table_descriptor->GetTupleArray(), oid, v, t->xc, &new_obj_ptr);
      auto *updater_xc = t->xc;
      auto *ptr = oa->get(oid);
start_over:
      fat_ptr head = volatile_read(*ptr);
      ASSERT(head.asi_type() == 0);
      Object *old_desc = (Object*)head.offset();
      ASSERT(old_desc);
      ASSERT(head.size_code() != INVALID_SIZE_CODE);
      auto *version = (dbtuple*)old_desc->GetPayload();
      bool overwrite = false;
      auto csn = old_desc->GetCSN();
      if (csn == NULL_PTR) {
        co_await std::suspend_always{};
        goto start_over;
      } else if (csn.asi_type() == fat_ptr::ASI_XID) {
        auto holder_xid = XID::from_ptr(csn);
        XID updater_xid = volatile_read(updater_xc->owner);
        if (holder_xid == updater_xid) {
          overwrite = true;
          goto install;
        }
wait_for_commit:
        TXN::xid_context *holder = TXN::xid_get_context(holder_xid);
        if (!holder) {
          ASSERT(old_desc->GetCSN().asi_type() == fat_ptr::ASI_CSN || oidmgr->oid_get(oa, oid) != head);
          goto start_over;
        }
        auto state = volatile_read(holder->state);
        auto owner = volatile_read(holder->owner);
        if (unlikely(owner != holder_xid)) {
          goto start_over;
        }
        ASSERT(holder_xid != updater_xid);
        if (state == TXN::TXN_COMMITTING) {
          co_await std::suspend_always{};
          goto wait_for_commit;
        }
        if (state == TXN::TXN_CMMTD) {
          auto holder_csn = volatile_read(holder->end);
          // >= RC can only update if we can see the latest version
          if (holder_csn >= updater_xc->begin) {
            goto end;
          }
          ASSERT(holder_xid != updater_xid);
          goto install;
        }
        goto end;
      } else {
        ASSERT(csn.asi_type() == fat_ptr::ASI_CSN);
        if (CSN::from_ptr(csn).offset() >= updater_xc->begin) {
          goto end;
        }
        goto install;
      }
install:
      new_obj_ptr = Object::Create(v, updater_xc->begin_epoch);
      ASSERT(new_obj_ptr.asi_type() == 0);
      Object *new_obj = (Object*)new_obj_ptr.offset();
      new_obj->SetCSN(updater_xc->owner.to_ptr());
      if (overwrite) {
        new_obj->SetNextPersistent(old_desc->GetNextPersistent());
        new_obj->SetNextVolatile(old_desc->GetNextVolatile());
        volatile_write(ptr->_ptr, new_obj_ptr._ptr);
        __sync_synchronize();
        prev_obj_ptr = head;
        goto end;
      } else {
        fat_ptr pa = old_desc->GetPersistentAddress();
        new_obj->SetNextPersistent(pa);
        new_obj->SetNextVolatile(head);
        if (__sync_bool_compare_and_swap(&ptr->_ptr, head._ptr, new_obj_ptr._ptr)) {
          // Succeeded installing a new version, now only I can modify the
          // chain, try recycle some objects
          if (config::enable_gc) {
            MM::gc_version_chain(ptr);
          }
          prev_obj_ptr = head;
          goto end;
        } else {
          MM::deallocate(new_obj_ptr);
        }
      }
    }
end:
    Object *prev_obj = (Object*)prev_obj_ptr.offset();
    if (prev_obj) {
      auto *tuple = (dbtuple*)((Object*)new_obj_ptr.offset())->GetPayload();
      fat_ptr prev_csn = prev_obj->GetCSN();
      if (!(prev_csn.asi_type() == fat_ptr::ASI_XID &&
          XID::from_ptr(prev_csn) == t->xid)) {
        t->add_to_pim_write_set(
          fat_ptr{(uint64_t)oa->get(oid)}, // indexonly: store &fat_ptr
          index_id, table_descriptor->GetTupleFid(), oid, tuple->size, false);
      }
      co_return {RC_TRUE};
    }
    else {
      co_return {RC_ABORT_SI_CONFLICT};
    }
  }
  else {
    co_return {RC_FALSE};
  }
}
#endif // defined(OLTPIM_OFFLOAD_INDEX_ONLY)

}

#endif // defined(OLTPIM)