#if defined(OLTPIM)
#include <mutex>
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

void register_index(ConcurrentMasstreeIndex *index) {
  registered_indexes.push_back(index);
}

void finalize_index_setup() {
  oltpim::engine::config config = {
    .num_ranks_per_numa_node = (int)FLAGS_oltpim_num_ranks_per_numa_node
  };
  oltpim::engine::g_engine.init(config);
  uint32_t num_pims = oltpim::engine::g_engine.num_pims();
  for (auto *index: registered_indexes) {
    index->set_num_pims(num_pims);
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

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_InsertRecord(transaction *t, const uint64_t &key, varstr &value) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());
  auto *xc = t->xc;
  fat_ptr new_obj = Object::Create(&value, xc->begin_epoch);

  args_insert_t args;
  args.index_id = index_id;
  args.key = key;
  args.value = new_obj._ptr;
  args.xid = xc->owner._val;
  args.csn = xc->begin;
  rets_insert_t rets;
  oltpim::request req(
    request_type_insert, &args, &rets, sizeof(args_insert_t), req_insert_rets_size(&args)
  );
  int pim_id = pim_id_of(key);
  oltpim::engine::g_engine.push(pim_id, &req);
  while (!oltpim::engine::g_engine.is_done(&req)) {
    co_await std::suspend_always{};
  }
  if (rets.status != STATUS_SUCCESS) {
    MM::deallocate(new_obj);
    uint16_t rc = (rets.status == STATUS_FAILED) ? RC_FALSE : RC_ABORT_SI_CONFLICT;
    co_return {rc};
  }
  dbtuple *tuple = (dbtuple*)((Object*)new_obj.offset())->GetPayload();
  t->add_to_pim_write_set(new_obj, index_id, pim_id, rets.oid, tuple->size, true);
  co_return {RC_TRUE};
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
  if (rets.status != STATUS_SUCCESS) {
    rc_t rc = (rets.status == STATUS_FAILED) ? RC_FALSE : RC_ABORT_SI_CONFLICT;
    co_return {rc};
  }
  t->add_to_pim_write_set(NULL_PTR, index_id, pim_id, rets.oid, 0, false);
  co_return {RC_TRUE};
}

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::pim_Scan(transaction *t, const uint64_t &start_key, const uint64_t *end_key,
                              ScanCallback &callback, uint32_t max_keys) {
  // TODO
  co_return {RC_FALSE};
}

}

#endif // defined(OLTPIM)