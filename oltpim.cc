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
    .enable_gc = (ermia::config::enable_gc != 0)
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
    const char *index_name, uint64_t pim_bits, uint64_t numa_bits) {
  ((ermia::ConcurrentMasstreeIndex*)
    ermia::TableDescriptor::GetIndex(index_name))->set_key_interval(pim_bits, numa_bits);
}

} // namespace pim

static int g_concurrent_masstree_index_id = 0;
static std::mutex g_concurrent_masstree_index_id_assign_mutex;

void ConcurrentMasstreeIndex::assign_index_id() {
  std::unique_lock lck(g_concurrent_masstree_index_id_assign_mutex);
  index_id = g_concurrent_masstree_index_id;
  ++g_concurrent_masstree_index_id;
}

void
ConcurrentMasstreeIndex::pim_GetRecordBegin(transaction *t, const uint64_t &key, void *req_) {
  ALWAYS_ASSERT(IsPrimary());
  auto *xc = t->xc;
  auto *req = (oltpim::request_get*)req_;
  auto &args = req->args;
  args.index_id = index_id;
  args.oid_query = 0;
  args.key = key;
  args.xid = (xc->owner._val) >> 16;
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
  auto status = rets.status;
  CHECK_VALID_STATUS(status);
  rc_t rc;
  if (status != STATUS_SUCCESS) {
    rc = (status == STATUS_FAILED) ? rc_t{RC_FALSE} : rc_t{RC_ABORT_SI_CONFLICT};
    co_return rc;
  }
  fat_ptr obj = {rets.value};
  dbtuple *tuple = ((Object*)obj.offset())->GetPinnedTuple(t);
  value.p = tuple->get_value_start();
  value.l = tuple->size;
  co_return rc_t{RC_TRUE};
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_GetRecord(transaction *t, const uint64_t &key, varstr &value) {
  auto *xc = t->xc;
  rc_t rc;

  oltpim::request_get req;
  auto &args = req.args;
  args.index_id = index_id;
  args.oid_query = 0;
  args.key = key;
  args.xid = (xc->owner._val) >> 16;
  args.csn = xc->begin;
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, &req);
  while (!oltpim::engine::g_engine.is_done(&req)) {
    co_await std::suspend_always{};
  }
  auto &rets = req.rets;

  if (!IsPrimary()) { // Secondary
    // Query again with the same req struct
    pim_id = (int)SVALUE_GET_PIMID(rets.value);
    uint32_t local_oid = SVALUE_GET_OID(rets.value);
    args.index_id = ((ConcurrentMasstreeIndex*)table_descriptor->GetPrimaryIndex())->index_id;
    args.oid_query = 1;
    args.key = (uint64_t)local_oid;
    args.xid = (xc->owner._val) >> 16;
    args.csn = xc->begin;
    // reuse req; already points to correct args and rets
    oltpim::engine::g_engine.push(pim_id, &req);
    while (!oltpim::engine::g_engine.is_done(&req)) {
      co_await std::suspend_always{};
    }
  }

  CHECK_VALID_STATUS(rets.status);
  if (rets.status != STATUS_SUCCESS) {
    rc = (rets.status == STATUS_FAILED) ? rc_t{RC_FALSE} : rc_t{RC_ABORT_SI_CONFLICT};
    co_return rc;
  }
  fat_ptr obj = {rets.value};
  dbtuple *tuple = ((Object*)obj.offset())->GetPinnedTuple(t);
  value.p = tuple->get_value_start();
  value.l = tuple->size;
  rc = tuple->size > 0 ? rc_t{RC_TRUE} : rc_t{RC_FALSE};
  co_return rc;
}

void
ConcurrentMasstreeIndex::pim_InsertRecordBegin(transaction *t, const uint64_t &key, varstr &value,
    void *req_, uint16_t *pim_id_) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());
  auto *xc = t->xc;
  fat_ptr new_obj = Object::Create(&value, xc->begin_epoch);
  auto *req = (oltpim::request_insert*)req_;
  auto &args = req->args;
  args.index_id = index_id;
  args.key = key;
  args.value = new_obj._ptr;
  args.xid = (xc->owner._val) >> 16;
  args.csn = xc->begin;
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, req);
  *pim_id_ = (uint16_t)pim_id;
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_InsertRecordEnd(transaction *t, void *req_, uint16_t pim_id, uint64_t *oid) {
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
  t->add_to_pim_write_set(new_obj, index_id, pim_id, rets.oid, tuple->size, true);
  if (oid) *oid = SVALUE_MAKE(pim_id, rets.oid);
  co_return {RC_TRUE};
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_InsertRecord(transaction *t, const uint64_t &key, varstr &value, uint64_t *oid) {
  oltpim::request_insert req;
  uint16_t pim_id;
  pim_InsertRecordBegin(t, key, value, &req, &pim_id);
  auto rc = co_await pim_InsertRecordEnd(t, &req, pim_id, oid);
  co_return rc;
}

void
ConcurrentMasstreeIndex::pim_InsertOIDBegin(transaction *t, const uint64_t &key, uint64_t oid, void *req_) {
  // For secondary index only
  ALWAYS_ASSERT(!IsPrimary());
  auto *xc = t->xc;
  auto *req = (oltpim::request_insert*)req_;
  auto &args = req->args;
  args.index_id = index_id;
  args.key = key;
  args.value = oid;
  args.xid = (xc->owner._val) >> 16;
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
  co_return {RC_TRUE};
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_InsertOID(transaction *t, const uint64_t &key, uint64_t oid) {
  oltpim::request_insert req;
  pim_InsertOIDBegin(t, key, oid, &req);
  auto rc = co_await pim_InsertOIDEnd(t, &req);
  co_return rc;
}

void
ConcurrentMasstreeIndex::pim_UpdateRecordBegin(
    transaction *t, const uint64_t &key, varstr &value, void *req_, uint16_t *pim_id_) {
  ALWAYS_ASSERT(IsPrimary());
  auto *xc = t->xc;
  fat_ptr new_obj = Object::Create(&value, xc->begin_epoch);
  auto *req = (oltpim::request_update*)req_;
  auto &args = req->args;
  args.index_id = index_id;
  args.key = key;
  args.new_value = new_obj._ptr;
  args.xid = (xc->owner._val) >> 16;
  args.csn = xc->begin;
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, req);
  *pim_id_ = (uint16_t)pim_id;
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_UpdateRecordEnd(transaction *t, void *req_, uint16_t pim_id) {
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
  // TODO manipulate new_value using old_value
  dbtuple *tuple = (dbtuple*)((Object*)new_obj.offset())->GetPayload();
  t->add_to_pim_write_set(new_obj, index_id, pim_id, rets.oid, tuple->size, false);
  co_return {RC_TRUE};
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_UpdateRecord(transaction *t, const uint64_t &key, varstr &value) {
  oltpim::request_update req;
  uint16_t pim_id;
  pim_UpdateRecordBegin(t, key, value, &req, &pim_id);
  auto rc = co_await pim_UpdateRecordEnd(t, &req, pim_id);
  co_return rc;
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_RemoveRecord(transaction *t, const uint64_t &key) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());
  auto *xc = t->xc;
  oltpim::request_remove req;
  auto &args = req.args;
  args.index_id = index_id;
  args.key = key;
  args.xid = (xc->owner._val) >> 16;
  args.csn = xc->begin;
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, &req);
  while (!oltpim::engine::g_engine.is_done(&req)) {
    co_await std::suspend_always{};
  }
  auto &rets = req.rets;
  CHECK_VALID_STATUS(rets.status);
  if (rets.status != STATUS_SUCCESS) {
    rc_t rc = (rets.status == STATUS_FAILED) ? RC_FALSE : RC_ABORT_SI_CONFLICT;
    co_return {rc};
  }
  t->add_to_pim_write_set(NULL_PTR, index_id, pim_id, rets.oid, 0, false);
  co_return {RC_TRUE};
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_Scan(transaction *t, const uint64_t &start_key, const uint64_t &end_key,
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
  const uint64_t key_interval = (1UL << key_interval_pim_bits);
  for (
      uint64_t begin_key = start_key & (~(key_interval - 1));
      begin_key <= end_key; begin_key += key_interval, ++cnt) {
    auto *req = (request_scan_base*)&scan_req[scan_req_size * cnt];
    auto &args = req->args;
    args.max_outs = max_keys_per_interval;
    args.index_id = index_id;
    args.keys[0] = max(begin_key, start_key);
    args.keys[1] = min(begin_key + key_interval - 1, end_key);
    args.xid = xid;
    args.csn = csn;
    oltpim::engine::g_engine.push(pim_id_of(begin_key), req);
  }
  ASSERT(cnt <= callback.num_intervals());
  for (int i = 0; i < cnt; ++i) {
    auto *req = (request_scan_base*)&scan_req[scan_req_size * i];
    while (!oltpim::engine::g_engine.is_done(req)) {
      co_await std::suspend_always{};
    }
  }
  for (int i = 0; i < cnt; ++i) {
    auto *req = (request_scan_base*)&scan_req[scan_req_size * i];
    auto status = req->rets.base.status;
    CHECK_VALID_STATUS(status);
    if (status != STATUS_SUCCESS) {
      uint16_t rc = (status == STATUS_FAILED) ? RC_FALSE : RC_ABORT_SI_CONFLICT;
      co_return {rc};
    }
  }

  if (!IsPrimary()) { // Secondary
    // Query again
    const uint8_t primary_index_id = ((ConcurrentMasstreeIndex*)table_descriptor->GetPrimaryIndex())->index_id;
    auto *get_reqs = (oltpim::request_get*)callback.get_req_storage();
    int cnt2 = 0;
    for (int i = 0; i < cnt; ++i) {
      auto *req = (request_scan_base*)&scan_req[scan_req_size * i];
      auto &rets = req->rets;
      for (uint32_t j = 0; j < rets.base.outs; ++j) {
        const uint64_t ret_value = rets.values[j];
        auto &args = get_reqs[cnt2].args;
        args.index_id = primary_index_id;
        args.oid_query = 1;
        args.key = (uint64_t)SVALUE_GET_OID(ret_value);
        args.xid = xid;
        args.csn = csn;
        oltpim::engine::g_engine.push((int)SVALUE_GET_PIMID(ret_value), &get_reqs[cnt2]);
        ++cnt2;
      }
    }
    ASSERT(cnt2 <= max_keys_per_interval * cnt);
    for (int i = 0; i < cnt2; ++i) {
      while (!oltpim::engine::g_engine.is_done(&get_reqs[i])) {
        co_await std::suspend_always{};
      }
    }
    for (int i = 0; i < cnt2; ++i) {
      auto status = get_reqs[i].rets.status;
      CHECK_VALID_STATUS(status);
      if (status != STATUS_SUCCESS) {
        uint16_t rc = (status == STATUS_FAILED) ? RC_FALSE : RC_ABORT_SI_CONFLICT;
        co_return {rc};
      }
    }
    for (int i = 0; i < cnt2; ++i) {
      fat_ptr obj = {get_reqs[i].rets.value};
      dbtuple *tuple = ((Object*)obj.offset())->GetPinnedTuple(t);
      varstr value(tuple->get_value_start(), tuple->size);
      if (!callback.Invoke(value)) break;
    }
    co_return {RC_TRUE};
  }
  else { // Primary
    for (int i = 0; i < cnt; ++i) {
      auto *req = (request_scan_base*)&scan_req[scan_req_size * i];
      auto &rets = req->rets;
      for (uint32_t j = 0; j < rets.base.outs; ++j) {
        fat_ptr obj = {rets.values[j]};
        dbtuple *tuple = ((Object*)obj.offset())->GetPinnedTuple(t);
        varstr value(tuple->get_value_start(), tuple->size);
        if (!callback.Invoke(value)) break;
      }
    }
    co_return {RC_TRUE};
  }
}



}

#endif // defined(OLTPIM)