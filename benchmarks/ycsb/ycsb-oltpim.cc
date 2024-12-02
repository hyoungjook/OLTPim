/*
 * A YCSB implementation based off of Silo's and equivalent to FOEDUS's.
 */
#if !defined(NESTED_COROUTINE)
#include "../dbtest.h"
#include "ycsb.h"
#include "unordered_set"
#include <gflags/gflags.h>

DEFINE_bool(ycsb_oltpim_multiget, false, "Use multiget for YCSB oltpim benchmark");
DEFINE_bool(ycsb_oltpim_numa_local_key, false, "Use numa-local key for YCSB oltpim");

extern YcsbWorkload ycsb_workload;
extern ReadTransactionType g_read_txn_type;
extern thread_local ermia::epoch_num coroutine_batch_end_epoch;

// You should re-compile this if you want different ops_per_hot_txn
static constexpr int ops_per_hot_txn_const = 10;
class ycsb_oltpim_worker : public ycsb_base_worker {
 public:
  ycsb_oltpim_worker(
      unsigned int worker_id, unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      spin_barrier *barrier_a, spin_barrier *barrier_b)
      : ycsb_base_worker(worker_id, seed, db, open_tables, barrier_a, barrier_b) {
    num_numa_nodes = numa_max_node() + 1;
    if (FLAGS_ycsb_oltpim_numa_local_key) {
      oltpim::engine::g_engine.optimize_for_numa_local_key();
    }
  }

  virtual void MyWork(char *) override {
    ALWAYS_ASSERT(is_worker);
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
      LOG(FATAL) << "Not implemented";
    }

    LOG_IF(FATAL, ops_per_hot_txn_const != FLAGS_ycsb_ops_per_hot_tx)
        << "Recompile with matching ops_per_hot_txn_const in ycsb-oltpim.cc";

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
      w.push_back(workload_desc("0-Insert", double(ycsb_workload.insert_percent()) / 100.0, nullptr, nullptr, TxnInsert));
    }

    if (ycsb_workload.update_percent()) {
      w.push_back(workload_desc("0-HotUpdate", FLAGS_ycsb_hot_tx_percent * double(ycsb_workload.update_percent()) / 100.0, nullptr, nullptr, TxnHotUpdate));
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

  static ermia::coro::task<rc_t> TxnHotUpdate(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_hot_update(txn, idx);
  }

  static ermia::coro::task<rc_t> TxnColdUpdate(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<ycsb_oltpim_worker *>(w)->txn_cold_update(txn, idx);
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
        uint64_t pim_key = rng_gen_key(true);
        if (FLAGS_ycsb_oltpim_numa_local_key) {
          pim_key = make_numa_local_key(pim_key);
        }
        rc = co_await table_index->pim_GetRecord(txn, pim_key, v);
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
      rc_t rc = co_await db->Commit(txn);
      TryCatchOltpim(rc);
    }

    co_return {RC_TRUE};
  }

  ermia::coro::task<rc_t> txn_hot_read_multiget(ermia::transaction *txn, uint32_t idx) {
    oltpim::request_get reqs[ops_per_hot_txn_const];
    for (int j = 0; j < FLAGS_ycsb_ops_per_hot_tx; ++j) {
      // TODO(tzwang): add read/write_all_fields knobs
      
      if (!ermia::config::index_probe_only) {
        uint64_t pim_key = rng_gen_key(true);
        if (FLAGS_ycsb_oltpim_numa_local_key) {
          pim_key = make_numa_local_key(pim_key);
        }
        table_index->pim_GetRecordBegin(txn, pim_key, &reqs[j]);
      }
      else {
        ALWAYS_ASSERT(false);
      }
    }
    for (int j = 0; j < FLAGS_ycsb_ops_per_hot_tx; ++j) {
      ermia::varstr &v = str(arenas[idx], sizeof(ycsb_kv::value));
      rc_t rc = rc_t{RC_INVALID};
      rc = co_await table_index->pim_GetRecordEnd(txn, v, &reqs[j]);

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
      rc_t rc = co_await db->Commit(txn);
      TryCatchOltpim(rc);
    }

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
    ALWAYS_ASSERT(false);
    co_return {RC_TRUE};
  }

  ermia::coro::task<rc_t> txn_cold_update(ermia::transaction *txn, uint32_t idx) {
    ALWAYS_ASSERT(false);
    co_return {RC_TRUE};
  }

  ermia::coro::task<rc_t> txn_hot_update(ermia::transaction *txn, uint32_t idx) {
    for (int i = 0; i < FLAGS_ycsb_update_per_tx; ++i) {
      ermia::varstr &v = str(arenas[idx], sizeof(ycsb_kv::value));
      new (v.data()) ycsb_kv::value("a");
      uint64_t pim_key = rng_gen_key(true);
      if (FLAGS_ycsb_oltpim_numa_local_key) {
        pim_key = make_numa_local_key(pim_key);
      }
      auto rc = co_await table_index->pim_UpdateRecord(txn, pim_key, v);
      TryCatchOltpim(rc);
    }

#ifndef CORO_BATCH_COMMIT
    rc_t rc = co_await db->Commit(txn);
    TryCatchOltpim(rc);
#endif
    co_return {RC_TRUE};
  }

 private:
  uint32_t _coro_batch_size;

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
    transactions = (ermia::transaction *)malloc(sizeof(ermia::transaction) * batch_size);
    arenas = (ermia::str_arena *)numa_alloc_onnode(sizeof(ermia::str_arena) * batch_size, numa_node_of_cpu(sched_getcpu()));
    for (auto i = 0; i < batch_size; ++i) {
      new (arenas + i) ermia::str_arena(ermia::config::arena_size_mb);
    }

    barrier_a->count_down();
    barrier_b->wait_for();

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
          rc = co_await db->Commit(&transactions[i]);
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
          std::get<0>(task_queue[i]).resume();
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
            rc = co_await db->Commit(&transactions[coro_task_id]);
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
                rc = co_await db->Commit(&transactions[coro_task_id]);
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

  uint16_t num_numa_nodes;
  uint64_t make_numa_local_key(uint64_t pim_key) {
    uint16_t numa_id = me->node;
    uint64_t lower = pim_key & ((1UL << YCSB_TABLE_NUMA_BITS) - 1);
    uint64_t upper = pim_key >> YCSB_TABLE_NUMA_BITS;
    // make upper % num_numa_nodes == numa_id
    upper = upper + numa_id - (upper % num_numa_nodes);
    uint64_t new_key = (upper << YCSB_TABLE_NUMA_BITS) | lower;
    if (new_key >= FLAGS_ycsb_hot_table_size) {
      new_key -= num_numa_nodes * (1UL << YCSB_TABLE_NUMA_BITS);
    }
    return new_key;
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
