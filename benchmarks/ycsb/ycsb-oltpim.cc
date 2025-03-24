/*
 * A YCSB implementation based off of Silo's and equivalent to FOEDUS's.
 */
#if !defined(NESTED_COROUTINE)
#include "../dbtest.h"
#include "ycsb.h"
#include "unordered_set"
#include <gflags/gflags.h>

DEFINE_bool(ycsb_oltpim_multiget, false, "Use multiget for YCSB oltpim benchmark");

extern YcsbWorkload ycsb_workload;
extern ReadTransactionType g_read_txn_type;
extern thread_local ermia::epoch_num coroutine_batch_end_epoch;

/* pim scan callback */
class pim_ycsb_scan_callback : public ermia::pim::PIMScanCallback {
private:
  static constexpr unsigned max_outs_per_interval_ = 16;
  static constexpr unsigned num_intervals_ = 2;
  // max_scan_length<=key_interval, then num_intervals is at most 2.
  // It's 1 if key range exactly fits in an interval, otherwise 2.
  using request_scan = typename oltpim::request_scan<max_outs_per_interval_>::t;
  request_scan scan_reqs[num_intervals_];
public:
  pim_ycsb_scan_callback(): n(0) {}
  uint32_t max_outs_per_interval() {return max_outs_per_interval_;}
  uint32_t num_intervals() {return num_intervals_;}
  void *scan_req_storage() {return (void*)&scan_reqs;}
  size_t scan_req_storage_size() {return sizeof(request_scan);}
  void *get_req_storage() {return nullptr;}

  int n;
  bool Invoke(const ermia::varstr &value) {
    memcpy(value_buf, value.data(), sizeof(ycsb_kv::value));
    n++;
    return true;
  }
  size_t size() {return n;}
  unsigned char value_buf[sizeof(ycsb_kv::value)];
};

// You should re-compile this if you want different ops_per_hot_txn
static constexpr int ops_per_hot_txn_const = 10;
class ycsb_oltpim_worker : public ycsb_base_worker {
 public:
  ycsb_oltpim_worker(
      unsigned int worker_id, unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      spin_barrier *barrier_a, spin_barrier *barrier_b, std::atomic<uint64_t> *next_inserts)
      : ycsb_base_worker(worker_id, seed, db, open_tables, barrier_a, barrier_b, next_inserts) {
    if (FLAGS_ycsb_numa_local) {
      oltpim::engine::g_engine.optimize_for_numa_local_key();
    }
  }

  virtual void MyWork(char *) override {
    ALWAYS_ASSERT(is_worker);
    if (FLAGS_ycsb_numa_local) assign_numa_local_table_index();
    tlog = ermia::GetLog();
    workload = get_workload();
    txn_counts.resize(workload.size());
    _coro_batch_size = ermia::config::coro_batch_size;

    auto schedule_mode = ermia::config::coro_scheduler;
    LOG_IF(FATAL, ermia::config::io_threads + ermia::config::remote_threads > ermia::config::worker_threads) << "Not enough threads.";
    if (ermia::config::io_threads || ermia::config::remote_threads) {
      if (worker_id < ermia::config::io_threads) {
        workload = get_cold_workload();
        schedule_mode = ermia::config::coro_io_scheduler;
        _coro_batch_size = ermia::config::coro_io_batch_size;
      } else if (worker_id < ermia::config::io_threads + ermia::config::remote_threads) {
        workload = get_remote_workload();
        schedule_mode = ermia::config::coro_remote_scheduler;
        _coro_batch_size = ermia::config::coro_remote_batch_size;
      } else {
        workload = get_hot_workload();
      }
    }
    if (schedule_mode == 0) {
      HybridBatch();
    } else if (schedule_mode == 1) {
      HybridPipeline();
    } else if (schedule_mode == 2) {
      HybridMosaicDB();
    } else {
      LOG(FATAL) << "\n-coro_scheduler=<0|1|2|3>"
                    "\n0: batch scheduler"
                    "\n1: pipeline scheduler"
                    "\n2: dual-queue pipeline (MosaicDB) scheduler";
    }
  }

  virtual workload_desc_vec get_workload() const override {
    workload_desc_vec w;
    if (ycsb_workload.scan_percent()) {
      w.push_back(workload_desc("0-Scan", double(ycsb_workload.scan_percent()) / 100.0, nullptr, nullptr,
        (FLAGS_ycsb_oltpim_multiget ? TxnScanMultiGet : TxnScan)));
      LOG_IF(FATAL, FLAGS_ycsb_max_scan_size > 16) <<
        "OLTPim YCSB scan currently supports max scan size up to 16.";
    }

    if (FLAGS_ycsb_oltpim_multiget) {
      LOG_IF(FATAL,
             !(FLAGS_ycsb_ops_per_hot_tx == ops_per_hot_txn_const &&
               FLAGS_ycsb_update_per_tx == ops_per_hot_txn_const &&
               FLAGS_ycsb_ins_per_tx == ops_per_hot_txn_const))
          << "Recompile with matching ops_per_hot_txn_const in ycsb-oltpim.cc";
    }

    LOG_IF(FATAL, g_read_txn_type != ReadTransactionType::HybridCoro) << "Read txn type must be hybrid-coro";

    if (ycsb_workload.read_percent()) {
      w.push_back(workload_desc("0-HotRead", FLAGS_ycsb_hot_tx_percent * double(ycsb_workload.read_percent()) / 100.0, nullptr, nullptr,
        (FLAGS_ycsb_oltpim_multiget ? TxnHotReadMultiGet : TxnHotRead)));
      w.push_back(workload_desc("1-ColdRead", (1 - FLAGS_ycsb_hot_tx_percent - FLAGS_ycsb_remote_tx_percent) * double(ycsb_workload.read_percent()) / 100.0, nullptr, nullptr, TxnRead));
      w.push_back(workload_desc("2-RemoteRead", FLAGS_ycsb_remote_tx_percent * double(ycsb_workload.read_percent()) / 100.0, nullptr, nullptr, TxnRemoteRead));
    }

    if (ycsb_workload.rmw_percent()) {
      LOG_IF(FATAL, ermia::config::index_probe_only) << "Not supported";
      w.push_back(workload_desc("0-HotRMW", FLAGS_ycsb_hot_tx_percent * double(ycsb_workload.rmw_percent()) / 100.0, nullptr, nullptr, TxnHotRMW));
      w.push_back(workload_desc("1-ColdRMW", (1 - FLAGS_ycsb_hot_tx_percent - FLAGS_ycsb_remote_tx_percent) * double(ycsb_workload.rmw_percent()) / 100.0, nullptr, nullptr, TxnRMW));
    }

    if (ycsb_workload.insert_percent()) {
      w.push_back(workload_desc("0-Insert", double(ycsb_workload.insert_percent()) / 100.0, nullptr, nullptr,
        (FLAGS_ycsb_oltpim_multiget ? TxnInsertMultiget : TxnInsert)));
    }

    if (ycsb_workload.update_percent()) {
      w.push_back(workload_desc("0-HotUpdate", FLAGS_ycsb_hot_tx_percent * double(ycsb_workload.update_percent()) / 100.0, nullptr, nullptr,
        (FLAGS_ycsb_oltpim_multiget ? TxnHotUpdateMultiGet : TxnHotUpdate)));
      w.push_back(workload_desc("1-ColdUpdate", (1 - FLAGS_ycsb_hot_tx_percent) * double(ycsb_workload.update_percent()) / 100.0, nullptr, nullptr, TxnColdUpdate));
    }

    return w;
  }

  workload_desc_vec get_hot_workload() const {
    ALWAYS_ASSERT(false);
    workload_desc_vec w;

    LOG_IF(FATAL, g_read_txn_type != ReadTransactionType::HybridCoro)
        << "Read txn type must be hybrid-coro";

    if (ycsb_workload.read_percent()) {
      w.push_back(workload_desc("0-HotRead", 1, nullptr, nullptr, TxnHotRead));
      w.push_back(workload_desc("1-ColdRead", 0, nullptr, nullptr, TxnRead));
      w.push_back(workload_desc("2-RemoteRead", 0, nullptr, nullptr, TxnRemoteRead));
    }

    if (ycsb_workload.rmw_percent()) {
      LOG_IF(FATAL, ermia::config::index_probe_only) << "Not supported";
      w.push_back(workload_desc("0-HotRMW", 1, nullptr, nullptr, TxnHotRMW));
      w.push_back(workload_desc("1-ColdRMW", 0, nullptr, nullptr, TxnRMW));
    }

    return w;
  }

  workload_desc_vec get_remote_workload() const {
    ALWAYS_ASSERT(false);
    workload_desc_vec w;

    LOG_IF(FATAL, g_read_txn_type != ReadTransactionType::HybridCoro)
        << "Read txn type must be hybrid-coro";

    if (ycsb_workload.read_percent()) {
      w.push_back(workload_desc("0-HotRead", 0, nullptr, nullptr, TxnHotRead));
      w.push_back(workload_desc("1-ColdRead", 0, nullptr, nullptr, TxnRead));
      w.push_back(workload_desc("2-RemoteRead", 1, nullptr, nullptr, TxnRemoteRead));
    }

    if (ycsb_workload.rmw_percent()) {
      LOG(FATAL) << "Not supported";
    }

    return w;
  }

  workload_desc_vec get_cold_workload() const {
    ALWAYS_ASSERT(false);
    workload_desc_vec w;

    LOG_IF(FATAL, g_read_txn_type != ReadTransactionType::HybridCoro)
        << "Read txn type must be hybrid-coro";

    if (ycsb_workload.read_percent()) {
      w.push_back(workload_desc("0-HotRead", 0, nullptr, nullptr, TxnHotRead));
      w.push_back(workload_desc("1-ColdRead", 1, nullptr, nullptr, TxnRead));
      w.push_back(workload_desc("2-RemoteRead", 0, nullptr, nullptr, TxnRemoteRead));
    }

    if (ycsb_workload.rmw_percent()) {
      LOG_IF(FATAL, ermia::config::index_probe_only) << "Not supported";
      w.push_back(workload_desc("0-HotRMW", 0, nullptr, nullptr, TxnHotRMW));
      w.push_back(workload_desc("1-ColdRMW", 1, nullptr, nullptr, TxnRMW));
    }

    return w;
  }

  static ermia::coro::task<rc_t> TxnRead(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_read(txn, idx);
  }

  static ermia::coro::task<rc_t> TxnHotRead(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_hot_read(txn, idx);
  }

  static ermia::coro::task<rc_t> TxnHotReadMultiGet(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_hot_read_multiget(txn, idx);
  }

  static ermia::coro::task<rc_t> TxnRemoteRead(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_remote_read(txn, idx);
  }

  static ermia::coro::task<rc_t> TxnRMW(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_rmw(txn, idx);
  }

  static ermia::coro::task<rc_t> TxnHotRMW(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_hot_rmw(txn, idx);
  }

  static ermia::coro::task<rc_t> TxnInsert(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_insert(txn, idx);
  }

  static ermia::coro::task<rc_t> TxnInsertMultiget(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_insert_multiget(txn, idx);
  }

  static ermia::coro::task<rc_t> TxnHotUpdate(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_hot_update(txn, idx);
  }

  static ermia::coro::task<rc_t> TxnHotUpdateMultiGet(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_hot_update_multiget(txn, idx);
  }

  static ermia::coro::task<rc_t> TxnColdUpdate(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_cold_update(txn, idx);
  }

  static ermia::coro::task<rc_t> TxnScan(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_scan(txn, idx);
  }

  static ermia::coro::task<rc_t> TxnScanMultiGet(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_scan_multiget(txn, idx);
  }

  /**
   * Read transaction with hot transactions going with 2-level coroutine
   * and cold transactions going with fully-nested coroutine.
   */
  ermia::coro::task<rc_t> txn_read(ermia::transaction *txn, uint32_t idx) {
    ALWAYS_ASSERT(false);
    co_return {RC_TRUE};
  }

  /**
   * Read transaction with hot transactions going with 2-level coroutine
   * and cold transactions going with fully-nested coroutine.
   */
  ermia::coro::task<rc_t> txn_hot_read(ermia::transaction *txn, uint32_t idx) {
    for (int j = 0; j < FLAGS_ycsb_ops_per_hot_tx; ++j) {
      // ermia::varstr &v = str(sizeof(ycsb_kv::value));
      ermia::varstr &v = str(arenas[idx], sizeof(ycsb_kv::value));

      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      if (!ermia::config::index_probe_only) {
        rc = co_await table_index->pim_GetRecord(txn, rng_gen_key(true), v);
      } else {
        ALWAYS_ASSERT(false);
      }

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatchOltpim(rc);
#else
      // Under SI this must succeed
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(ermia::config::index_probe_only || *(char *)v.data() == 'a');
#endif

      if (!ermia::config::index_probe_only) {
        memcpy((char *)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(ycsb_kv::value));
        ALWAYS_ASSERT(*(char *)v.data() == 'a');
      }
    }

    if (!ermia::config::index_probe_only) {
      rc_t rc = co_await txn->oltpim_commit();
      TryCatchOltpim(rc);
    }

    co_return {RC_TRUE};
  }

  ermia::coro::task<rc_t> txn_hot_read_multiget(ermia::transaction *txn, uint32_t idx) {
    ASSERT(!ermia::config::index_probe_only);
    oltpim::request_get reqs[ops_per_hot_txn_const];
    for (int j = 0; j < FLAGS_ycsb_ops_per_hot_tx; ++j) {
      table_index->pim_GetRecordBegin(txn, rng_gen_key(true), &reqs[j]);
    }
    rc_t rc;
    ermia::varstr &v = str(arenas[idx], sizeof(ycsb_kv::value));
    for (int j = 0; j < FLAGS_ycsb_ops_per_hot_tx; ++j) {
      rc = co_await table_index->pim_GetRecordEnd(txn, v, &reqs[j], &_oltpim_idle_wait);
      // Under SI this must succeed
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      memcpy((char *)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(ycsb_kv::value));
      ALWAYS_ASSERT(*(char *)v.data() == 'a');
    }
    rc = co_await txn->oltpim_commit();
    TryCatchOltpim(rc);
    co_return {RC_TRUE};
  }

  /**
   * Read transaction with hot transactions going with 2-level coroutine
   * and cold transactions going with fully-nested coroutine.
   */
  ermia::coro::task<rc_t> txn_remote_read(ermia::transaction *txn, uint32_t idx) {
    ALWAYS_ASSERT(false);
    co_return {RC_TRUE};
  }

  // Read-modify-write transaction with context-switch using simple coroutine
  ermia::coro::task<rc_t> txn_rmw(ermia::transaction *txn, uint32_t idx) {
    ALWAYS_ASSERT(false);
    co_return {RC_TRUE};
  }

  ermia::coro::task<rc_t> txn_hot_rmw(ermia::transaction *txn, uint32_t idx) {
    ALWAYS_ASSERT(false);
    co_return {RC_TRUE};
  }

  ermia::coro::task<rc_t> txn_insert(ermia::transaction *txn, uint32_t idx) {
    ermia::varstr &v = str(arenas[idx], sizeof(ycsb_kv::value));
    for (int i = 0; i < FLAGS_ycsb_ins_per_tx; ++i) {
      *(char *)v.p = 'a';
      auto rc = co_await table_index->pim_InsertRecord(txn, gen_new_key(), v);
      TryCatchOltpim(rc);
    }

#ifndef CORO_BATCH_COMMIT
    rc_t rc = co_await txn->oltpim_commit();
    TryCatchOltpim(rc);
#endif
    co_return {RC_TRUE};
  }

  ermia::coro::task<rc_t> txn_insert_multiget(ermia::transaction *txn, uint32_t idx) {
    oltpim::request_insert reqs[ops_per_hot_txn_const];
    ermia::varstr &v = str(arenas[idx], sizeof(ycsb_kv::value));
    for (int j = 0; j < FLAGS_ycsb_ins_per_tx; ++j) {
      *(char *)v.p = 'a';
      table_index->pim_InsertRecordBegin(txn, gen_new_key(), v, &reqs[j]);
    }
    rc_t rc{RC_TRUE};
    for (int j = 0; j < FLAGS_ycsb_ins_per_tx; ++j) {
      // Txn should wait until all requests are done
      // because the engine is using the reqs stack variable
      // and it is released if this function returns.
      rc_t _rc = co_await table_index->pim_InsertRecordEnd(txn, &reqs[j]);
      if (_rc._val != RC_TRUE) rc = _rc;
    }
    TryCatchOltpim(rc);

#ifndef CORO_BATCH_COMMIT
    rc = co_await txn->oltpim_commit();
    TryCatchOltpim(rc);
#endif
    co_return {RC_TRUE};
  }

  ermia::coro::task<rc_t> txn_cold_update(ermia::transaction *txn, uint32_t idx) {
    ALWAYS_ASSERT(false);
    co_return {RC_TRUE};
  }

  ermia::coro::task<rc_t> txn_hot_update(ermia::transaction *txn, uint32_t idx) {
    ermia::varstr &v = str(arenas[idx], sizeof(ycsb_kv::value));
    for (int i = 0; i < FLAGS_ycsb_update_per_tx; ++i) {
      new (v.data()) ycsb_kv::value("a");
      auto rc = co_await table_index->pim_UpdateRecord(txn, rng_gen_key(true), v);
      TryCatchOltpim(rc);
    }

#ifndef CORO_BATCH_COMMIT
    rc_t rc = co_await txn->oltpim_commit();
    TryCatchOltpim(rc);
#endif
    co_return {RC_TRUE};
  }

  ermia::coro::task<rc_t> txn_hot_update_multiget(ermia::transaction *txn, uint32_t idx) {
    oltpim::request_update reqs[ops_per_hot_txn_const];
    ermia::varstr &v = str(arenas[idx], sizeof(ycsb_kv::value));
    for (int j = 0; j < FLAGS_ycsb_update_per_tx; ++j) {
      new (v.data()) ycsb_kv::value("a");
      table_index->pim_UpdateRecordBegin(txn, rng_gen_key(true), v, &reqs[j]);
    }
    rc_t rc{RC_TRUE};
    for (int j = 0; j < FLAGS_ycsb_update_per_tx; ++j) {
      // Txn should wait until all requests are done
      // because the engine is using the reqs stack variable
      // and it is released if this function returns.
      rc_t _rc = co_await table_index->pim_UpdateRecordEnd(txn, &reqs[j], &_oltpim_idle_wait);
      if (_rc._val != RC_TRUE) rc = _rc;
    }
    TryCatchOltpim(rc);

#ifndef CORO_BATCH_COMMIT
    rc = co_await txn->oltpim_commit();
    TryCatchOltpim(rc);
#endif
    co_return {RC_TRUE};
  }

  ermia::coro::task<rc_t> txn_scan(ermia::transaction *txn, uint32_t idx) {
    rc_t rc;
    for (int i = 0; i < FLAGS_ycsb_ops_per_tx; ++i) {
      uint64_t start_key = rng_gen_key(true);
      uint64_t end_key = start_key + rng_gen_scan_length() - 1;
      pim_ycsb_scan_callback callback;
      rc = co_await table_index->pim_Scan(
        txn, start_key, end_key, callback, callback.max_outs_per_interval());
      ALWAYS_ASSERT(callback.size() <= FLAGS_ycsb_max_scan_size);
      ALWAYS_ASSERT(rc._val == RC_TRUE);
    }

#ifndef CORO_BATCH_COMMIT
    rc = co_await txn->oltpim_commit();
    TryCatchOltpim(rc);
#endif
    co_return {RC_TRUE};
  }

  ermia::coro::task<rc_t> txn_scan_multiget(ermia::transaction *txn, uint32_t idx) {
    pim_ycsb_scan_callback callbacks[ops_per_hot_txn_const];
    rc_t rc;
    for (int j = 0; j < FLAGS_ycsb_ops_per_tx; ++j) {
      uint64_t start_key = rng_gen_key(true);
      uint64_t end_key = start_key + rng_gen_scan_length() - 1;
      table_index->pim_ScanBegin(
        txn, start_key, end_key, callbacks[j], callbacks[j].max_outs_per_interval());
    }
    for (int j = 0; j < FLAGS_ycsb_ops_per_tx; ++j) {
      rc = co_await table_index->pim_ScanEnd(
        txn, callbacks[j], callbacks[j].max_outs_per_interval());
      ALWAYS_ASSERT(callbacks[j].size() <= FLAGS_ycsb_max_scan_size);
      ALWAYS_ASSERT(rc._val == RC_TRUE);
    }

#ifndef CORO_BATCH_COMMIT
    rc = co_await txn->oltpim_commit();
    TryCatchOltpim(rc);
#endif
    co_return {RC_TRUE};
  }

 private:
  uint32_t _coro_batch_size;
  bool _oltpim_idle_wait;

  /**
   * This scheduler processes transactions in a batch fashion.
   */
  void HybridBatch() {
    const size_t batch_size = _coro_batch_size;
    std::vector<std::tuple<ermia::coro::task<rc_t>, ermia::transaction *>> task_queue(batch_size);
    std::vector<uint32_t> task_workload_idxs(batch_size);
    transactions = (ermia::transaction *)malloc(sizeof(ermia::transaction) * batch_size);
    arenas = (ermia::str_arena *)numa_alloc_onnode(sizeof(ermia::str_arena) * batch_size, numa_node_of_cpu(sched_getcpu()));
    for (auto i = 0; i < batch_size; ++i) {
      new (arenas + i) ermia::str_arena(ermia::config::arena_size_mb);
    }

    barrier_a->count_down();
    barrier_b->wait_for();

    while (running) {
      ermia::epoch_num begin_epoch = ermia::MM::epoch_enter();
      util::timer t;

      for (uint32_t i = 0; i < batch_size; i++) {
        ermia::coro::task<rc_t> &coro_task = std::get<0>(task_queue[i]);
        ASSERT(!coro_task.valid());

        uint32_t workload_idx = fetch_workload();
        task_workload_idxs[i] = workload_idx;
        ASSERT(workload[workload_idx].task_fn);

        ermia::transaction *txn = nullptr;
        if (!ermia::config::index_probe_only) {
          if (workload[workload_idx].name == "0-HotRMW" || workload[workload_idx].name == "0-Insert" ||
              workload[workload_idx].name == "0-HotUpdate") {
            txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[i], &transactions[i], i);
          } else {
            txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[i], &transactions[i], i);
          }
          txn->set_user_data(i);
          ermia::TXN::xid_context *xc = txn->GetXIDContext();
          xc->begin_epoch = 0;
        } else {
          arenas[i].reset();
        }
        task_queue[i] = std::make_tuple(workload[workload_idx].task_fn(this, txn, i), txn);
        std::get<0>(task_queue[i]).start();
      }

      bool batch_completed = false;
      while (!batch_completed) {
        batch_completed = true;
        for (uint32_t i = 0; i < batch_size; i++) {
          if (!std::get<0>(task_queue[i]).valid()) {
            continue;
          }

          ermia::transaction *txn = std::get<1>(task_queue[i]);
          if (!std::get<0>(task_queue[i]).done()) {
            batch_completed = false;
            if (unlikely(txn->is_cold())) {
              int tid = -1;
              int ret_val = -1;
              tlog->peek_tid(tid, ret_val);
              if (tid >= 0 && ret_val == std::get<1>(task_queue[tid])->get_expected_io_size()) {
                std::get<0>(task_queue[tid]).resume();
              }
            } else {
              std::get<0>(task_queue[i]).resume();
            }
          } else {
            finish_workload(std::get<0>(task_queue[i]).get_return_value(), task_workload_idxs[i], t);
            task_queue[i] = std::make_tuple(ermia::coro::task<rc_t>(nullptr), nullptr);
          }
        }
      }
      ermia::MM::epoch_exit(coroutine_batch_end_epoch, begin_epoch);
    }
  }

  /**
   * This pipeline scheduler has one queue.
   */
  void HybridPipeline() {
#ifdef GROUP_SAME_TRX
    LOG(FATAL) << "Pipeline scheduler doesn't work with batching same-type transactions";
#endif

    LOG(INFO) << "Epoch management and latency recorder in Pipeline scheduler are not logically correct";

    const size_t batch_size = _coro_batch_size;
    std::vector<std::tuple<ermia::coro::task<rc_t>, ermia::transaction *>> task_queue(batch_size);
    std::vector<uint32_t> task_workload_idxs(batch_size);
    std::unordered_set<uint32_t> cold_txn_set;
    util::timer *ts = (util::timer *)numa_alloc_onnode(sizeof(util::timer) * batch_size, numa_node_of_cpu(sched_getcpu()));
    transactions = (ermia::transaction *)numa_alloc_onnode(sizeof(ermia::transaction) * batch_size, numa_node_of_cpu(sched_getcpu()));
    arenas = (ermia::str_arena *)numa_alloc_onnode(sizeof(ermia::str_arena) * batch_size, numa_node_of_cpu(sched_getcpu()));
    for (auto i = 0; i < batch_size; ++i) {
      new (arenas + i) ermia::str_arena(ermia::config::arena_size_mb);
    }

    barrier_a->count_down();
    barrier_b->wait_for();
    uint64_t t0 = (uint64_t)-1;

    ermia::epoch_num begin_epoch = ermia::MM::epoch_enter();

    for (uint32_t i = 0; i < batch_size; i++) {
      uint32_t workload_idx = fetch_workload();
      task_workload_idxs[i] = workload_idx;
      ASSERT(workload[workload_idx].task_fn);
      ermia::transaction *txn = nullptr;
      if (!ermia::config::index_probe_only) {
        if (workload[workload_idx].name == "0-HotRMW" || workload[workload_idx].name == "0-Insert" ||
            workload[workload_idx].name == "0-HotUpdate") {
          txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[i], &transactions[i], i);
        } else {
          txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[i], &transactions[i], i);
        }
        txn->set_user_data(i);
        ermia::TXN::xid_context *xc = txn->GetXIDContext();
        xc->begin_epoch = 0;
      } else {
        arenas[i].reset();
      }

      task_queue[i] = std::make_tuple(workload[workload_idx].task_fn(this, txn, i), txn);
      new (&ts[i]) util::timer();
      std::get<0>(task_queue[i]).start();
    }

    uint32_t i = 0;
    while (running) {
      if (std::get<0>(task_queue[i]).done()) {
        rc_t rc = std::get<0>(task_queue[i]).get_return_value();
#ifdef CORO_BATCH_COMMIT
        if (!rc.IsAbort()) {
          rc = co_await transactions[i].oltpim_commit();
        }
#endif
        finish_workload(rc, task_workload_idxs[i], ts[i]);
        task_queue[i] = std::make_tuple(ermia::coro::task<rc_t>(nullptr), nullptr);
        uint32_t workload_idx = fetch_workload();
        task_workload_idxs[i] = workload_idx;
        ASSERT(workload[workload_idx].task_fn);
        ermia::transaction *txn = nullptr;
        if (!ermia::config::index_probe_only) {
          if (workload[workload_idx].name == "3-RMW") {
            txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[i], &transactions[i], i);
          } else {
            txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[i], &transactions[i], i);
          }
          txn->set_user_data(i);
          if (cold_txn_set.size() >= ermia::config::coro_cold_tx_threshold) {
            txn->set_abort_if_cold(true);
          }
          ermia::TXN::xid_context *xc = txn->GetXIDContext();
          xc->begin_epoch = 0;
        } else {
          arenas[i].reset();
        }

        task_queue[i] = std::make_tuple(workload[workload_idx].task_fn(this, txn, i), txn);
        ts[i].lap();
        std::get<0>(task_queue[i]).start();
      } else {
        auto txn = std::get<1>(task_queue[i]);
        if (txn->is_cold()) {
          cold_txn_set.insert(i);
          int tid = -1;
          int ret_val = -1;
          tlog->peek_tid(tid, ret_val);
          if (tid >= 0 && ret_val == std::get<1>(task_queue[tid])->get_expected_io_size()) {
            cold_txn_set.erase(tid);
            std::get<0>(task_queue[tid]).resume();
          }
        } else {
          uint64_t t0 = oltpim::cur_us();
          std::get<0>(task_queue[i]).resume();
          if (_oltpim_idle_wait) oltpim_cpu_idle_time_us += (oltpim::cur_us() - t0);
        }
      }

      i = (i + 1) & (batch_size - 1);
    }
    ermia::MM::epoch_exit(coroutine_batch_end_epoch, begin_epoch);
  }

  /**
   * This pipeline scheduler has two queues (i.e., hot and cold) AND staging. The staging list enforces FIFO policy.
   * Coroutine tasks that are blocked by I/O will be moved to the cold queue. When an on-disk operation finishes in the cold queue,
   * the transaction it belongs to is moved to the staging queue, which later will be scheduled back to the hot queue,
   * because the next operation still starts from probing the index, after which we will see if this operation eventually is hot or cold.
   * When the cold queue is full, the system will abort new cold transactions.
   */
  void HybridMosaicDB() {
#ifdef GROUP_SAME_TRX
    LOG(FATAL) << "Pipeline scheduler doesn't work with batching same-type transactions";
#endif

    LOG(INFO) << "Epoch management and latency recorder in Pipeline scheduler are not logically correct";

    uint64_t hot_queue_size = _coro_batch_size;
    uint64_t cold_queue_size = ermia::config::coro_cold_queue_size;
    uint64_t task_vec_size = hot_queue_size + cold_queue_size;
    std::list<uint64_t> next_free_task_id_queue;
    std::list<uint64_t> next_free_cold_queue_idx;

    std::vector<std::tuple<ermia::coro::task<rc_t>, ermia::transaction *, uint64_t>> hot_queue(hot_queue_size);
    std::vector<std::tuple<ermia::coro::task<rc_t>, ermia::transaction *, uint64_t>> cold_queue(cold_queue_size);
    std::list<std::tuple<ermia::coro::task<rc_t>, ermia::transaction *, uint64_t>> staging_queue;

    std::vector<uint64_t> task_workload_idxs(task_vec_size);
    util::timer *ts = (util::timer *)numa_alloc_onnode(sizeof(util::timer) * task_vec_size, numa_node_of_cpu(sched_getcpu()));
    arenas = (ermia::str_arena *)numa_alloc_onnode(sizeof(ermia::str_arena) * task_vec_size, numa_node_of_cpu(sched_getcpu()));
    for (int i = 0; i < task_vec_size; ++i) {
      new (&ts[i]) util::timer();
      new (arenas + i) ermia::str_arena(ermia::config::arena_size_mb);
      next_free_task_id_queue.push_back(i);
    }
    for (int i = 0; i < cold_queue_size; ++i) {
      next_free_cold_queue_idx.push_back(i);
    }
    transactions = (ermia::transaction *)malloc(sizeof(ermia::transaction) * task_vec_size);

    barrier_a->count_down();
    barrier_b->wait_for();

    ermia::epoch_num begin_epoch = ermia::MM::epoch_enter();

    uint64_t next_free_tid = 0;
    for (uint64_t i = 0; i < hot_queue_size; i++) {
      ASSERT(next_free_task_id_queue.size());
      uint64_t workload_idx = fetch_workload();
      ASSERT(workload[workload_idx].task_fn);
      next_free_tid = next_free_task_id_queue.front();
      next_free_task_id_queue.pop_front();
      task_workload_idxs[next_free_tid] = workload_idx;
      ermia::transaction *txn = nullptr;
      if (!ermia::config::index_probe_only) {
        if (workload[workload_idx].name == "3-RMW") {
          txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
        } else {
          txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
        }
        txn->set_user_data(next_free_tid);
        ermia::TXN::xid_context *xc = txn->GetXIDContext();
        xc->begin_epoch = 0;
      } else {
        arenas[next_free_tid].reset();
      }

      hot_queue[i] = std::make_tuple(workload[workload_idx].task_fn(this, txn, next_free_tid), txn, next_free_tid);
      ts[i].lap();
      std::get<0>(hot_queue[i]).start();
    }

    uint64_t hot_queue_idx = 0;
    uint64_t cold_queue_idx = 0;
    uint64_t hot_txn_count = hot_queue_size;
    uint64_t hot_txn_commit = 0;
    while (running) {
      auto coro_task_txn = std::get<1>(hot_queue[hot_queue_idx]);
      auto coro_task_id = std::get<2>(hot_queue[hot_queue_idx]);

      if (hot_txn_count && !coro_task_txn) {
        hot_queue_idx = (hot_queue_idx + 1) & (hot_queue_size - 1);
        continue;
      }

      if (hot_txn_count == 0 || std::get<0>(hot_queue[hot_queue_idx]).done()) {
        if (hot_txn_count) {
          rc_t rc = std::get<0>(hot_queue[hot_queue_idx]).get_return_value();

#ifdef CORO_BATCH_COMMIT
          if (!rc.IsAbort()) {
            rc = co_await transactions[coro_task_id].oltpim_commit();
          }
#endif
          finish_workload(rc, task_workload_idxs[coro_task_id], ts[coro_task_id]);

          hot_queue[hot_queue_idx] = std::make_tuple(ermia::coro::task<rc_t>(nullptr), nullptr, ~uint64_t{0});
          next_free_task_id_queue.push_front(coro_task_id);
          --hot_txn_count;
          ++hot_txn_commit;
        }

        // (0) When there is an empty slot in the hot queue,
        //     we first check if the interval is up or the hot queue is empty,
        //     if so, we go check on the cold queue.
        if (next_free_task_id_queue.size() < cold_queue_size && (hot_txn_commit > ermia::config::coro_check_cold_tx_interval || hot_txn_count == 0)) {
coldq:
          hot_txn_commit = 0;
          for (cold_queue_idx = 0; cold_queue_idx < cold_queue_size; ++cold_queue_idx) {
            if (!std::get<1>(cold_queue[cold_queue_idx])) {
              continue;
            }

            coro_task_txn = std::get<1>(cold_queue[cold_queue_idx]);
            coro_task_id = std::get<2>(cold_queue[cold_queue_idx]);

            if (std::get<0>(cold_queue[cold_queue_idx]).done()) {
              // (1) Check if there is any txn in the cold queue that can be committed.
              rc_t rc = std::get<0>(cold_queue[cold_queue_idx]).get_return_value();
#ifdef CORO_BATCH_COMMIT
              if (!rc.IsAbort()) {
                rc = co_await transactions[coro_task_id].oltpim_commit();
              }
#endif
              finish_workload(rc, task_workload_idxs[coro_task_id], ts[coro_task_id]);
              cold_queue[cold_queue_idx] = std::make_tuple(ermia::coro::task<rc_t>(nullptr), nullptr, ~uint64_t{0});
              next_free_task_id_queue.push_front(coro_task_id);
              next_free_cold_queue_idx.push_front(cold_queue_idx);
            } else if (!coro_task_txn->is_cold()) {
              // (2) Check if there is any txn in the cold queue that needs to be moved the staging queue,
              //     because its next operation starts from in-memory index probing.
              staging_queue.push_back(std::move(cold_queue[cold_queue_idx]));
              cold_queue[cold_queue_idx] = std::make_tuple(ermia::coro::task<rc_t>(nullptr), nullptr, ~uint64_t{0});
              next_free_cold_queue_idx.push_front(cold_queue_idx);
            } else {
              // (3) Peek the uring once to resume the next available txn.
              //     Note, the order of the completed I/O request in CQE is random,
              //     therefore we cannot simply resume the txn that the cold queue index currently is pointing to.
              int tid = -1;
              int ret_val = -1;
              tlog->peek_tid(tid, ret_val);
              if (tid >= 0 && ret_val == transactions[tid].get_expected_io_size()) {
                std::get<0>(cold_queue[transactions[tid].index()]).resume();
              }
            }
          }
        }

        // (4) We need to fetch a workload regardless. We prioritze the transactions in the staging queue, if any.
        //     If there is no transaction in the staging list, we need to check if the system is closed to new transactions,
        //     i.e., the threshold is hit. If not, we then fetch a new transaction.
        if (staging_queue.size()) {
          hot_queue[hot_queue_idx] = std::move(staging_queue.front());
          staging_queue.pop_front();
          ++hot_txn_count;
          std::get<0>(hot_queue[hot_queue_idx]).resume();
        } else {
          uint16_t workload_idx = fetch_workload();
          next_free_tid = next_free_task_id_queue.front();
          next_free_task_id_queue.pop_front();
          task_workload_idxs[next_free_tid] = workload_idx;
          ASSERT(workload[workload_idx].task_fn);
          ermia::transaction *txn = nullptr;
          if (!ermia::config::index_probe_only) {
            if (workload[workload_idx].name == "3-RMW") {
              txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
            } else {
              txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
            }
            txn->set_user_data(next_free_tid);
            if (next_free_cold_queue_idx.size() < hot_queue_size) {
              txn->set_abort_if_cold(true);
            }
            ermia::TXN::xid_context *xc = txn->GetXIDContext();
            xc->begin_epoch = 0;
          } else {
            arenas[next_free_tid].reset();
          }
          hot_queue[hot_queue_idx] = std::make_tuple(workload[workload_idx].task_fn(this, txn, next_free_tid), txn, next_free_tid);
          ++hot_txn_count;
          ts[next_free_tid].lap();
          std::get<0>(hot_queue[hot_queue_idx]).start();
        }

        if (hot_txn_count == 0) {
          goto coldq;
        }
      } else if (coro_task_txn->is_cold()) {
        // Move this task which is waiting for IO to complete to the cold queue.
        uint64_t next_cold_queue_idx = next_free_cold_queue_idx.front();
        next_free_cold_queue_idx.pop_front();
        coro_task_txn->set_index(next_cold_queue_idx);
        cold_queue[next_cold_queue_idx] = std::move(hot_queue[hot_queue_idx]);
        --hot_txn_count;
        hot_queue[hot_queue_idx] = std::make_tuple(ermia::coro::task<rc_t>(nullptr), nullptr, ~uint64_t{0});

        // Then, fetch a workload from the staging queue, if any. Otherwise, fetch a new one.
        if (staging_queue.size()) {
          hot_queue[hot_queue_idx] = std::move(staging_queue.front());
          staging_queue.pop_front();
          ++hot_txn_count;
          std::get<0>(hot_queue[hot_queue_idx]).resume();
        } else {
          uint16_t workload_idx = fetch_workload();
          next_free_tid = next_free_task_id_queue.front();
          next_free_task_id_queue.pop_front();
          task_workload_idxs[next_free_tid] = workload_idx;
          ASSERT(workload[workload_idx].task_fn);
          ermia::transaction *txn = nullptr;
          if (!ermia::config::index_probe_only) {
            if (workload[workload_idx].name == "3-RMW") {
              txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
            } else {
              txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
            }
            txn->set_user_data(next_free_tid);
            if (next_free_cold_queue_idx.size() < hot_queue_size) {
              txn->set_abort_if_cold(true);
            }
            ermia::TXN::xid_context *xc = txn->GetXIDContext();
            xc->begin_epoch = 0;
          } else {
            arenas[next_free_tid].reset();
          }
          hot_queue[hot_queue_idx] = std::make_tuple(workload[workload_idx].task_fn(this, txn, next_free_tid), txn, next_free_tid);
          ++hot_txn_count;
          ts[next_free_tid].lap();
          std::get<0>(hot_queue[hot_queue_idx]).start();
        }
      } else {
        std::get<0>(hot_queue[hot_queue_idx]).resume();
      }

      hot_queue_idx = (hot_queue_idx + 1) & (hot_queue_size - 1);
    }
    ermia::MM::epoch_exit(coroutine_batch_end_epoch, begin_epoch);
  }
};

void ycsb_oltpim_do_test(ermia::Engine *db) {
  ycsb_parse_options();
  ycsb_bench_runner<ycsb_oltpim_worker> r(db);
  r.run();
}

int main(int argc, char **argv) {
  bench_main(argc, argv, ycsb_oltpim_do_test);
  return 0;
}

#endif  // HYBRID_COROUTINE
