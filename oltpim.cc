#if defined(OLTPIM)
#include <mutex>
#include <random>
#include <gflags/gflags.h>
#include "engine.h"
#include "txn.h"
#include "sm-table.h"
#include "engine.hpp"
#include "interface.h"

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
    .num_ranks_per_numa_node = (int)FLAGS_oltpim_num_ranks_per_numa_node
  };
  oltpim::engine::g_engine.init(config);
  uint32_t num_pims = oltpim::engine::g_engine.num_pims();
  std::uniform_int_distribution<uint32_t> rand_offset(0, num_pims - 1);
  for (auto *index: registered_indexes) {
    index->set_num_pims(num_pims, rand_offset(rand_gen));
  }
}

void set_index_partition_interval(const char *index_name, uint64_t interval) {
  ((ermia::ConcurrentMasstreeIndex*)
    ermia::TableDescriptor::GetIndex(index_name))->set_key_interval(interval);
}

}

static int g_concurrent_masstree_index_id = 0;
static std::mutex g_concurrent_masstree_index_id_assign_mutex;

void ConcurrentMasstreeIndex::assign_index_id() {
  std::unique_lock lck(g_concurrent_masstree_index_id_assign_mutex);
  index_id = g_concurrent_masstree_index_id;
  ++g_concurrent_masstree_index_id;
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_GetRecord(transaction *t, const uint64_t &key, varstr &value) {
  auto *xc = t->xc;
  rc_t rc;

  args_get_t args;
  args.index_id = index_id;
  args.oid_query = 0;
  args.key = key;
  args.xid = xc->owner._val;
  args.csn = xc->begin;
  rets_get_t rets;
  oltpim::request req(
    request_type_get, &args, &rets, sizeof(args_get_t), req_get_rets_size(&args)
  );
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, &req);
  while (!oltpim::engine::g_engine.is_done(&req)) {
    co_await std::suspend_always{};
  }

  if (!IsPrimary()) { // Secondary
    // Query again
    pim_id = (int)SVALUE_GET_PIMID(rets.value);
    uint32_t local_oid = SVALUE_GET_OID(rets.value);
    args.index_id = ((ConcurrentMasstreeIndex*)table_descriptor->GetPrimaryIndex())->index_id;
    args.oid_query = 1;
    args.key = (uint64_t)local_oid;
    args.xid = xc->owner._val;
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
    void *arg_, void *ret_, void *req_, uint16_t *pim_id_) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());
  auto *xc = t->xc;
  fat_ptr new_obj = Object::Create(&value, xc->begin_epoch);

  auto *arg = (args_insert_t*)arg_;
  arg->index_id = index_id;
  arg->key = key;
  arg->value = new_obj._ptr;
  arg->xid = xc->owner._val;
  arg->csn = xc->begin;
  auto *req = (oltpim::request*)req_;
  *req = oltpim::request(
    request_type_insert, arg_, ret_, sizeof(args_insert_t), req_insert_rets_size(arg)
  );
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, req);
  *pim_id_ = (uint16_t)pim_id;
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_InsertRecordEnd(transaction *t, void *req_, uint16_t pim_id, uint64_t *oid) {
  auto *req = (oltpim::request*)req_;
  while (!oltpim::engine::g_engine.is_done(req)) {
    co_await std::suspend_always{};
  }
  // recover new_obj from req->args->value
  fat_ptr new_obj = {((args_insert_t*)req->args)->value};
  auto *ret = (rets_insert_t*)req->rets;
  CHECK_VALID_STATUS(ret->status);
  if (ret->status != STATUS_SUCCESS) {
    MM::deallocate(new_obj);
    uint16_t rc = (ret->status == STATUS_FAILED) ? RC_FALSE : RC_ABORT_SI_CONFLICT;
    co_return {rc};
  }
  dbtuple *tuple = (dbtuple*)((Object*)new_obj.offset())->GetPayload();
  t->add_to_pim_write_set(new_obj, index_id, pim_id, ret->oid, tuple->size, true);
  if (oid) *oid = SVALUE_MAKE(pim_id, ret->oid);
  co_return {RC_TRUE};
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_InsertRecord(transaction *t, const uint64_t &key, varstr &value, uint64_t *oid) {
  args_insert_t arg;
  rets_insert_t ret;
  oltpim::request req;
  uint16_t pim_id;
  pim_InsertRecordBegin(t, key, value, &arg, &ret, &req, &pim_id);
  auto rc = co_await pim_InsertRecordEnd(t, &req, pim_id, oid);
  co_return rc;
}

void
ConcurrentMasstreeIndex::pim_InsertOIDBegin(transaction *t, const uint64_t &key, uint64_t oid,
    void *arg_, void *ret_, void *req_) {
  // For secondary index only
  ALWAYS_ASSERT(!IsPrimary());
  auto *xc = t->xc;

  auto *arg = (args_insert_t*)arg_;
  arg->index_id = index_id;
  arg->key = key;
  arg->value = oid;
  arg->xid = xc->owner._val;
  arg->csn = xc->begin;
  auto *req = (oltpim::request*)req_;
  *req = oltpim::request(
    request_type_insert, arg_, ret_, sizeof(args_insert_t), req_insert_rets_size(arg)
  );
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, req);
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_InsertOIDEnd(transaction *t, void *req_) {
  auto *req = (oltpim::request*)req_;
  while (!oltpim::engine::g_engine.is_done(req)) {
    co_await std::suspend_always{};
  }
  auto *ret = (rets_insert_t*)req->rets;
  CHECK_VALID_STATUS(ret->status);
  if (ret->status != STATUS_SUCCESS) {
    uint16_t rc = (ret->status == STATUS_FAILED) ? RC_FALSE : RC_ABORT_SI_CONFLICT;
    co_return {rc};
  }
  co_return {RC_TRUE};
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_InsertOID(transaction *t, const uint64_t &key, uint64_t oid) {
  args_insert_t arg;
  rets_insert_t ret;
  oltpim::request req;
  pim_InsertOIDBegin(t, key, oid, &arg, &ret, &req);
  auto rc = co_await pim_InsertOIDEnd(t, &req);
  co_return rc;
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_UpdateRecord(transaction *t, const uint64_t &key, varstr &value) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());
  auto *xc = t->xc;
  fat_ptr new_obj = Object::Create(&value, xc->begin_epoch);

  args_update_t args;
  args.index_id = index_id;
  args.key = key;
  args.new_value = new_obj._ptr;
  args.xid = xc->owner._val;
  args.csn = xc->begin;
  rets_update_t rets;
  oltpim::request req(
    request_type_update, &args, &rets, sizeof(args_update_t), req_update_rets_size(&args)
  );
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, &req);
  while (!oltpim::engine::g_engine.is_done(&req)) {
    co_await std::suspend_always{};
  }
  CHECK_VALID_STATUS(rets.status);
  if (rets.status != STATUS_SUCCESS) {
    MM::deallocate(new_obj);
    rc_t rc = (rets.status == STATUS_FAILED) ? RC_FALSE : RC_ABORT_SI_CONFLICT;
    co_return {rc};
  }
  // TODO manipulate new_value using old_value
  dbtuple *tuple = (dbtuple*)((Object*)new_obj.offset())->GetPayload();
  t->add_to_pim_write_set(new_obj, index_id, pim_id, rets.oid, tuple->size, false);
  co_return {RC_TRUE};
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_RemoveRecord(transaction *t, const uint64_t &key) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());
  auto *xc = t->xc;

  args_remove_t args;
  args.index_id = index_id;
  args.key = key;
  args.xid = xc->owner._val;
  args.csn = xc->begin;
  rets_remove_t rets;
  oltpim::request req(
    request_type_remove, &args, &rets, sizeof(args_remove_t), req_remove_rets_size(&args)
  );
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, &req);
  while (!oltpim::engine::g_engine.is_done(&req)) {
    co_await std::suspend_always{};
  }
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
  const uint64_t xid = xc->owner._val;
  const uint64_t csn = xc->begin;

  args_scan_t *const scan_args = (args_scan_t*)callback.scan_args_storage();
  uint8_t *const scan_rets = (uint8_t*)callback.scan_rets_storage();
  oltpim::request *const scan_reqs = (oltpim::request*)callback.scan_reqs_storage();
  const uint32_t scan_rets_size = callback.scan_rets_size();
  const int pim_id_end = pim_id_of(end_key);
  int cnt = 0;
  for (
      uint64_t begin_key = (start_key / key_partition_interval) * key_partition_interval;
      begin_key <= end_key; begin_key += key_partition_interval, ++cnt) {
    auto &arg = scan_args[cnt];
    arg.max_outs = max_keys_per_interval;
    arg.index_id = index_id;
    arg.keys[0] = max(begin_key, start_key);
    arg.keys[1] = min(begin_key + key_partition_interval - 1, end_key);
    arg.xid = xid;
    arg.csn = csn;
    scan_reqs[cnt] = oltpim::request(
      request_type_scan, &arg, &scan_rets[scan_rets_size * cnt],
      sizeof(args_scan_t), scan_rets_size
    );
    oltpim::engine::g_engine.push(pim_id_of(begin_key), &scan_reqs[cnt]);
  }
  ASSERT(cnt <= callback.num_intervals());
  for (int i = 0; i < cnt; ++i) {
    while (!oltpim::engine::g_engine.is_done(&scan_reqs[i])) {
      co_await std::suspend_always{};
    }
  }
  for (int i = 0; i < cnt; ++i) {
    auto status = ((rets_scan_t*)&scan_rets[scan_rets_size * i])->status;
    CHECK_VALID_STATUS(status);
    if (status != STATUS_SUCCESS) {
      uint16_t rc = (status == STATUS_FAILED) ? RC_FALSE : RC_ABORT_SI_CONFLICT;
      co_return {rc};
    }
  }

  if (!IsPrimary()) { // Secondary
    // Query again
    const uint8_t primary_index_id = ((ConcurrentMasstreeIndex*)table_descriptor->GetPrimaryIndex())->index_id;
    auto *get_reqs = (oltpim::request*)callback.get_reqs_storage();
    auto *get_args = (args_get_t*)callback.get_args_storage();
    auto *get_rets = (rets_get_t*)callback.get_rets_storage();
    int cnt2 = 0;
    for (int i = 0; i < cnt; ++i) {
      auto *ret = (rets_scan_t*)&scan_rets[scan_rets_size * i];
      for (uint32_t j = 0; j < ret->outs; ++j) {
        const uint64_t ret_value = ret->values[j];
        auto &arg = get_args[cnt2];
        arg.index_id = primary_index_id;
        arg.oid_query = 1;
        arg.key = (uint64_t)SVALUE_GET_OID(ret_value);
        arg.xid = xid;
        arg.csn = csn;
        get_reqs[cnt2] = oltpim::request(
          request_type_get, &arg, &get_rets[cnt2], sizeof(args_get_t), sizeof(rets_get_t)
        );
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
      auto status = get_rets[i].status;
      CHECK_VALID_STATUS(status);
      if (status != STATUS_SUCCESS) {
        uint16_t rc = (status == STATUS_FAILED) ? RC_FALSE : RC_ABORT_SI_CONFLICT;
        co_return {rc};
      }
    }
    for (int i = 0; i < cnt2; ++i) {
      fat_ptr obj = {get_rets[i].value};
      dbtuple *tuple = ((Object*)obj.offset())->GetPinnedTuple(t);
      varstr value(tuple->get_value_start(), tuple->size);
      if (!callback.Invoke(value)) break;
    }
    co_return {RC_TRUE};
  }
  else { // Primary
    for (int i = 0; i < cnt; ++i) {
      auto *ret = (rets_scan_t*)&scan_rets[scan_rets_size * i];
      for (uint32_t j = 0; j < ret->outs; ++j) {
        fat_ptr obj = {ret->values[j]};
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