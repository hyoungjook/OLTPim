/**
 * An implementation of TPC-C based off of:
 * https://github.com/oltpbenchmark/oltpbench/tree/master/src/com/oltpbenchmark/benchmarks/tpcc
 */

#if !defined(NESTED_COROUTINE)

#include "tpcc-config.h"
#include "tpcc-key64.h"
#include "../../oltpim.h"
#include "interface.h"

/* pim scan callback */
#define DECLARE_SCAN_RETS_SIZE(_1, name, _2, _3, _4, _5, \
                               rets_size, rets_size_supp, ...) \
  static constexpr inline uint32_t req_##name##_rets_size(const uint8_t __rn) { \
    return (rets_size) + (rets_size_supp); \
  }
REQUEST_TYPES_LIST(DECLARE_SCAN_RETS_SIZE)
#undef DECLARE_SCAN_RETS_SIZE

template <int max_outs_per_interval_, bool primary, int num_intervals_ = 1>
class pim_static_limit_callback : public ermia::pim::PIMScanCallback {
  // these are declared here in order to make
  // compile-decided-size variables in the stack; so that
  // we can use them together with coroutines.
private:
  static constexpr int scan_rets_size_ = req_scan_rets_size(max_outs_per_interval_);
  static constexpr int num_secondary_reqs = (primary ? 0 : max_outs_per_interval_ * num_intervals_);
  uint8_t scan_rets[scan_rets_size_ * num_intervals_];
  union storage_type {
    struct {
      args_scan_t args[num_intervals_];
      oltpim::request reqs[num_intervals_];
    } scan;
    struct {
      args_get_t args[num_secondary_reqs];
      rets_get_t rets[num_secondary_reqs];
      oltpim::request reqs[num_secondary_reqs];
    } get;
  };
  uint8_t storage_buf[sizeof(storage_type)];
  const storage_type *storage = (storage_type*)&storage_buf;

public:
  uint32_t max_outs_per_interval() {return max_outs_per_interval_;}
  uint32_t num_intervals() {return num_intervals_;}
  uint8_t *scan_args_storage() {return (uint8_t*)&storage->scan.args;}
  uint8_t *scan_rets_storage() {return (uint8_t*)&scan_rets;}
  uint32_t scan_rets_size() {return scan_rets_size_;}
  uint8_t *scan_reqs_storage() {return (uint8_t*)&storage->scan.reqs;}
  uint8_t *get_args_storage() {return (uint8_t*)&storage->get.args;}
  uint8_t *get_rets_storage() {return (uint8_t*)&storage->get.rets;}
  uint8_t *get_reqs_storage() {return (uint8_t*)&storage->get.reqs;}

public:
  std::vector<ermia::varstr> values;
  bool Invoke(const ermia::varstr &value) {
    values.push_back(value);
    return true; // no check limit here
  }
  size_t size() {return values.size();}
};

class tpcc_oltpim_worker : public bench_worker, public tpcc_worker_mixin {
 public:
  tpcc_oltpim_worker(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
                 const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                 const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions,
                 spin_barrier *barrier_a, spin_barrier *barrier_b,
                 uint home_warehouse_id)
      : bench_worker(worker_id, true, seed, db, open_tables, barrier_a, barrier_b),
        tpcc_worker_mixin(partitions),
        home_warehouse_id(home_warehouse_id) {
  ASSERT(home_warehouse_id >= 1 and home_warehouse_id <= NumWarehouses() + 1);
  memset(&last_no_o_ids[0], 0, sizeof(last_no_o_ids));
}

  // XXX(stephentu): tune this
  static const size_t NMaxCustomerIdxScanElems = 16;

  ermia::coro::task<rc_t> txn_new_order(ermia::transaction *txn, uint32_t idx);

  static ermia::coro::task<rc_t> TxnNewOrder(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<tpcc_oltpim_worker *>(w)->txn_new_order(txn, idx);
  }

  ermia::coro::task<rc_t> txn_delivery(ermia::transaction *txn, uint32_t idx);

  static ermia::coro::task<rc_t> TxnDelivery(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<tpcc_oltpim_worker *>(w)->txn_delivery(txn, idx);
  }

  ermia::coro::task<rc_t> txn_credit_check(ermia::transaction *txn, uint32_t idx);

  static ermia::coro::task<rc_t> TxnCreditCheck(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<tpcc_oltpim_worker *>(w)->txn_credit_check(txn, idx);
  }

  ermia::coro::task<rc_t> txn_payment(ermia::transaction *txn, uint32_t idx);

  static ermia::coro::task<rc_t> TxnPayment(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<tpcc_oltpim_worker *>(w)->txn_payment(txn, idx);
  }

  ermia::coro::task<rc_t> txn_order_status(ermia::transaction *txn, uint32_t idx);

  static ermia::coro::task<rc_t> TxnOrderStatus(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<tpcc_oltpim_worker *>(w)->txn_order_status(txn, idx);
  }

  ermia::coro::task<rc_t> txn_stock_level(ermia::transaction *txn, uint32_t idx);

  static ermia::coro::task<rc_t> TxnStockLevel(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<tpcc_oltpim_worker *>(w)->txn_stock_level(txn, idx);
  }

  ermia::coro::task<rc_t> txn_query2(ermia::transaction *txn, uint32_t idx);

  static ermia::coro::task<rc_t> TxnQuery2(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<tpcc_oltpim_worker *>(w)->txn_query2(txn, idx);
  }

  ermia::coro::task<rc_t> txn_microbench_random(ermia::transaction *txn, uint32_t idx);

  static ermia::coro::task<rc_t> TxnMicroBenchRandom(bench_worker *w, ermia::transaction *txn, uint32_t idx) {
    return static_cast<tpcc_oltpim_worker *>(w)->txn_microbench_random(txn, idx);
  }

  virtual workload_desc_vec get_workload() const override;
  virtual void MyWork(char *) override;
  workload_desc_vec get_cold_workload() const;

 protected:
  ALWAYS_INLINE ermia::varstr &str(ermia::str_arena &a, uint64_t size) { return *a.next(size); }

 private:
  const uint home_warehouse_id;
  int32_t last_no_o_ids[10];  // XXX(stephentu): hack
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
          if (workload[workload_idx].name == "OrderStatus" || workload[workload_idx].name == "StockLevel") {
            txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[i], &transactions[i], i);
          } else {
            txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[i], &transactions[i], i);
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
      ermia::MM::epoch_exit(0, begin_epoch);
    }
  }

  /**
   * This pipeline scheduler has one queue.
   */
  void HybridVanillaPipeline() {
#ifdef GROUP_SAME_TRX
    LOG(FATAL) << "Pipeline scheduler doesn't work with batching same-type transactions";
#endif

    LOG(INFO) << "Epoch management and latency recorder in Pipeline scheduler are not logically correct";

    const size_t batch_size = _coro_batch_size;
    std::vector<std::tuple<ermia::coro::task<rc_t>, ermia::transaction *>> task_queue(batch_size);
    std::vector<uint32_t> task_workload_idxs(batch_size);
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
      ermia::coro::task<rc_t> &coro_task = std::get<0>(task_queue[i]);
      ASSERT(!coro_task.valid());

      uint32_t workload_idx = fetch_workload();
      task_workload_idxs[i] = workload_idx;
      ASSERT(workload[workload_idx].task_fn);

      ermia::transaction *txn = nullptr;
      if (!ermia::config::index_probe_only) {
        if (workload[workload_idx].name == "OrderStatus" || workload[workload_idx].name == "StockLevel") {
          txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[i], &transactions[i], i);
        } else {
          txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[i], &transactions[i], i);
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
          if (workload[workload_idx].name == "OrderStatus" || workload[workload_idx].name == "StockLevel") {
            txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[i], &transactions[i], i);
          } else {
            txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[i], &transactions[i], i);
          }
          txn->set_user_data(i);
          ermia::TXN::xid_context *xc = txn->GetXIDContext();
          xc->begin_epoch = 0;
        } else {
          arenas[i].reset();
        }
        task_queue[i] = std::make_tuple(workload[workload_idx].task_fn(this, txn, i), txn);
        ts[i].lap();
        std::get<0>(task_queue[i]).start();
      } else {
        ermia::transaction *txn = std::get<1>(task_queue[i]);
        if (txn->is_cold()) {
          int tid = -1;
          int ret_val = -1;
          tlog->peek_tid(tid, ret_val);
          if (tid >= 0 && ret_val == std::get<1>(task_queue[tid])->get_expected_io_size()) {
            std::get<0>(task_queue[tid]).resume();
          }
        } else {
          std::get<0>(task_queue[i]).resume();
        }
      }

      i = (i + 1) & (batch_size - 1);
    }
    ermia::MM::epoch_exit(0, begin_epoch);
  }

  /**
   * This pipeline scheduler has one long queue with admission control.
   * There is a threshold on the number of cold transactions that the scheduler can admit.
   */
  void HybridSingleQPipelineAC() {
#ifdef GROUP_SAME_TRX
    LOG(FATAL) << "Pipeline scheduler doesn't work with batching same-type transactions";
#endif

    LOG(INFO) << "Epoch management and latency recorder in Pipeline scheduler are not logically correct";

    const size_t batch_size = _coro_batch_size;
    std::vector<std::tuple<ermia::coro::task<rc_t>, ermia::transaction *>> task_queue(batch_size);
    std::vector<uint32_t> task_workload_idxs(batch_size);
    util::timer *ts = (util::timer *)numa_alloc_onnode(sizeof(util::timer) * batch_size, numa_node_of_cpu(sched_getcpu()));
    transactions = (ermia::transaction *)malloc(sizeof(ermia::transaction) * batch_size);
    double cold_txn_count = 0;
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
      if (workload[workload_idx].name == "Read") {
        ++cold_txn_count;
      }
      ASSERT(workload[workload_idx].task_fn);
      ermia::transaction *txn = nullptr;
      if (!ermia::config::index_probe_only) {
        if (workload[workload_idx].name == "OrderStatus" || workload[workload_idx].name == "StockLevel") {
          txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[i], &transactions[i], i);
        } else {
          txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[i], &transactions[i], i);
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
        if (workload[task_workload_idxs[i]].name == "Read") {
          --cold_txn_count;
        }
        task_queue[i] = std::make_tuple(ermia::coro::task<rc_t>(nullptr), nullptr);
        uint32_t workload_idx = fetch_workload();
        while (cold_txn_count >= ermia::config::coro_cold_tx_threshold && workload[workload_idx].name == ermia::config::coro_cold_tx_name) {
          workload_idx = fetch_workload();
        }
        if (workload[workload_idx].name == "Read") {
          ++cold_txn_count;
        }
        task_workload_idxs[i] = workload_idx;
        ASSERT(workload[workload_idx].task_fn);
        ermia::transaction *txn = nullptr;
        if (!ermia::config::index_probe_only) {
          if (workload[workload_idx].name == "OrderStatus" || workload[workload_idx].name == "StockLevel") {
            txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[i], &transactions[i], i);
          } else {
            txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[i], &transactions[i], i);
          }
          txn->set_user_data(i);
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
          int tid = -1;
          int ret_val = -1;
          tlog->peek_tid(tid, ret_val);
          if (tid >= 0 && ret_val == std::get<1>(task_queue[tid])->get_expected_io_size()) {
            std::get<0>(task_queue[tid]).resume();
          }
        } else {
          std::get<0>(task_queue[i]).resume();
        }
      }

      i = (i + 1) & (batch_size - 1);
    }
    ermia::MM::epoch_exit(0, begin_epoch);
  }

  /**
   * This pipeline scheduler has two queues (i.e., hot and cold) AND staging. The staging list enforces FIFO policy.
   * Coroutine tasks that are blocked by I/O will be moved to the cold queue. When an on-disk operation finishes in the cold queue,
   * the transaction it belongs to is moved to the staging queue, which later will be scheduled back to the hot queue,
   * because the next operation still starts from probing the index, after which we will see if this operation eventually is hot or cold.
   * Since no admission control is in place, when the number of total transactions scheduled (in the hot and cold queues and
   * the staging list) gets to the number of total slots in the cold queue, the system is closed to new transactions until the number
   * of scheduled transactions is below the threshold.
   */
  void HybridDualQPipeline() {
#ifdef GROUP_SAME_TRX
    LOG(FATAL) << "Pipeline scheduler doesn't work with batching same-type transactions";
#endif

    LOG(INFO) << "Epoch management and latency recorder in Pipeline scheduler are not logically correct";

    uint64_t hot_queue_size = _coro_batch_size;
    uint64_t cold_queue_size = ermia::config::coro_cold_queue_size;
    LOG_IF(FATAL, cold_queue_size == 0) << "--coro_cold_queue_size cannot be 0 in dual-queue pipeline";
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
        if (workload[workload_idx].name == "OrderStatus" || workload[workload_idx].name == "StockLevel") {
          txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
        } else {
          txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
        }
        txn->set_user_data(next_free_tid);
        txn->set_position(true, i);
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
    uint64_t cold_txn_count = 0;
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
        if (cold_txn_count && (hot_txn_commit > ermia::config::coro_check_cold_tx_interval || hot_txn_count == 0)) {
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
              --cold_txn_count;
            } else if (!coro_task_txn->is_cold()) {
              // (2) Check if there is any txn in the cold queue that needs to be moved the staging queue,
              //     because its next operation starts from in-memory index probing.
              staging_queue.push_back(std::move(cold_queue[cold_queue_idx]));
              cold_queue[cold_queue_idx] = std::make_tuple(ermia::coro::task<rc_t>(nullptr), nullptr, ~uint64_t{0});
              next_free_cold_queue_idx.push_front(cold_queue_idx);
              --cold_txn_count;
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
        } else if (staging_queue.size() + hot_txn_count + cold_txn_count < cold_queue_size) {
          uint16_t workload_idx = fetch_workload();
          next_free_tid = next_free_task_id_queue.front();
          next_free_task_id_queue.pop_front();
          task_workload_idxs[next_free_tid] = workload_idx;
          ASSERT(workload[workload_idx].task_fn);
          ermia::transaction *txn = nullptr;
          if (!ermia::config::index_probe_only) {
            if (workload[workload_idx].name == "OrderStatus" || workload[workload_idx].name == "StockLevel") {
              txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
            } else {
              txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
            }
            txn->set_user_data(next_free_tid);
            txn->set_position(true, hot_queue_idx);
            ermia::TXN::xid_context *xc = txn->GetXIDContext();
            xc->begin_epoch = 0;
          }
          else {
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
        coro_task_txn->set_index(next_free_cold_queue_idx.front());
        cold_queue[next_free_cold_queue_idx.front()] = std::move(hot_queue[hot_queue_idx]);
        --hot_txn_count;
        ++cold_txn_count;
        next_free_cold_queue_idx.pop_front();
        hot_queue[hot_queue_idx] = std::make_tuple(ermia::coro::task<rc_t>(nullptr), nullptr, ~uint64_t{0});

        // Then, fetch a workload from the staging queue, if any. Otherwise, fetch a new one.
        if (staging_queue.size()) {
          hot_queue[hot_queue_idx] = std::move(staging_queue.front());
          staging_queue.pop_front();
          ++hot_txn_count;
          std::get<0>(hot_queue[hot_queue_idx]).resume();
        } else if (staging_queue.size() + hot_txn_count + cold_txn_count < cold_queue_size) {
          uint16_t workload_idx = fetch_workload();
          next_free_tid = next_free_task_id_queue.front();
          next_free_task_id_queue.pop_front();
          task_workload_idxs[next_free_tid] = workload_idx;
          ASSERT(workload[workload_idx].task_fn);
          ermia::transaction *txn = nullptr;
          if (!ermia::config::index_probe_only) {
            if (workload[workload_idx].name == "OrderStatus" || workload[workload_idx].name == "StockLevel") {
              txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
            } else {
              txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
            }
            txn->set_user_data(next_free_tid);
            txn->set_position(true, hot_queue_idx);
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
    ermia::MM::epoch_exit(0, begin_epoch);
  }

  /**
   * This pipeline scheduler has two queues (i.e., hot and cold) AND admission control AND staging.
   * Admission control will prevent the system from admitting more cold transactions when the cold
   * queue is full, regardless of the hot ratio.
   */
  void HybridDualQPipelineAC() {
#ifdef GROUP_SAME_TRX
    LOG(FATAL) << "Pipeline scheduler doesn't work with batching same-type transactions";
#endif

    LOG(INFO) << "Epoch management and latency recorder in Pipeline scheduler are not logically correct";

    uint64_t hot_queue_size = _coro_batch_size;
    uint64_t cold_queue_size = ermia::config::coro_cold_queue_size;
    LOG_IF(FATAL, cold_queue_size == 0) << "--coro_cold_queue_size cannot be 0 in dual-queue pipeline";
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
    uint64_t cold_txn_count = 0;
    for (uint64_t i = 0; i < hot_queue_size; i++) {
      ASSERT(next_free_task_id_queue.size());
      uint64_t workload_idx = fetch_workload();
      if (workload_idx == 1) {
        // if it is a cold transaction, i.e., "Read", we need to check if there is space in the cold
        // queue. Cold transactions in the hot queue also reserve slots, since they are not
        // immediately moved to the cold queue when generated.
        if (cold_txn_count == cold_queue_size) {
          workload_idx = 0;
        } else {
          ++cold_txn_count;
        }
      }
      ASSERT(workload[workload_idx].task_fn);
      next_free_tid = next_free_task_id_queue.front();
      next_free_task_id_queue.pop_front();
      task_workload_idxs[next_free_tid] = workload_idx;
      ermia::transaction *txn = nullptr;
      if (!ermia::config::index_probe_only) {
        if (workload[workload_idx].name == "OrderStatus" || workload[workload_idx].name == "StockLevel") {
          txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
        } else {
          txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
        }
        txn->set_user_data(next_free_tid);
        txn->set_position(true, i);
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

      if (std::get<0>(hot_queue[hot_queue_idx]).done()) {
        rc_t rc = std::get<0>(hot_queue[hot_queue_idx]).get_return_value();

#ifdef CORO_BATCH_COMMIT
        if (!rc.IsAbort()) {
          rc = co_await db->Commit(&transactions[coro_task_id]);
        }
#endif
        if (task_workload_idxs[coro_task_id] == 1) {
          --cold_txn_count;
        }
        finish_workload(rc, task_workload_idxs[coro_task_id], ts[coro_task_id]);

        hot_queue[hot_queue_idx] = std::make_tuple(ermia::coro::task<rc_t>(nullptr), nullptr, ~uint64_t{0});
        next_free_task_id_queue.push_front(coro_task_id);
        --hot_txn_count;
        ++hot_txn_commit;

        // (0) When there is an empty slot in the hot queue,
        //     we first check if the interval is up or the hot queue is empty,
        //     if so, we go check on the cold queue.
        if (cold_txn_count && (hot_txn_commit > ermia::config::coro_check_cold_tx_interval || hot_txn_count == 0)) {
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
              if (task_workload_idxs[coro_task_id] == 1) {
                --cold_txn_count;
              }
              finish_workload(rc, task_workload_idxs[coro_task_id], ts[coro_task_id]);
              cold_queue[cold_queue_idx] = std::make_tuple(ermia::coro::task<rc_t>(nullptr), nullptr, ~uint64_t{0});
              next_free_task_id_queue.push_front(coro_task_id);
              next_free_cold_queue_idx.push_front(cold_queue_idx);
            }
            else if (!coro_task_txn->is_cold()) {
              // (2) Check if there is any txn in the cold queue that needs to be moved the staging queue,
              //     because its next operation starts from in-memory index probing.
              staging_queue.push_back(std::move(cold_queue[cold_queue_idx]));
              cold_queue[cold_queue_idx] = std::make_tuple(ermia::coro::task<rc_t>(nullptr), nullptr, ~uint64_t{0});
              next_free_cold_queue_idx.push_front(cold_queue_idx);
            }
            else {
              // (3) Peek the uring once to resume the next available txn.
              //     Note, the order of the completed I/O request in CQE is random,
              //     therefore we cannot simply resume the txn that the cold queue index currently is referring to.
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
        //     Otherwise, fetch a new one.
        if (staging_queue.size()) {
          hot_queue[hot_queue_idx] = std::move(staging_queue.front());
          staging_queue.pop_front();
          std::get<0>(hot_queue[hot_queue_idx]).resume();
        } else {
          uint16_t workload_idx = fetch_workload();
          while (cold_txn_count == cold_queue_size && workload[workload_idx].name == "Read") {
            workload_idx = fetch_workload();
          }
          if (workload_idx == 1) {
            ++cold_txn_count;
          }
          next_free_tid = next_free_task_id_queue.front();
          next_free_task_id_queue.pop_front();
          task_workload_idxs[next_free_tid] = workload_idx;
          ASSERT(workload[workload_idx].task_fn);
          ermia::transaction *txn = nullptr;
          if (!ermia::config::index_probe_only) {
            if (workload[workload_idx].name == "OrderStatus" || workload[workload_idx].name == "StockLevel") {
              txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
            } else {
              txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
            }
            txn->set_user_data(next_free_tid);
            txn->set_position(true, hot_queue_idx);
            ermia::TXN::xid_context *xc = txn->GetXIDContext();
            xc->begin_epoch = 0;
          } else {
            arenas[next_free_tid].reset();
          }
          hot_queue[hot_queue_idx] = std::make_tuple(workload[workload_idx].task_fn(this, txn, next_free_tid), txn, next_free_tid);
          ts[next_free_tid].lap();
          std::get<0>(hot_queue[hot_queue_idx]).start();
        }

        ++hot_txn_count;
      } else if (coro_task_txn->is_cold()) {
        // Move this task which is waiting for IO to complete to the cold queue.
        coro_task_txn->set_index(next_free_cold_queue_idx.front());
        cold_queue[next_free_cold_queue_idx.front()] = std::move(hot_queue[hot_queue_idx]);
        --hot_txn_count;
        next_free_cold_queue_idx.pop_front();
        hot_queue[hot_queue_idx] = std::make_tuple(ermia::coro::task<rc_t>(nullptr), nullptr, ~uint64_t{0});

        // Then, fetch a workload from the staging queue, if any. Otherwise, fetch a new one.
        if (staging_queue.size()) {
          hot_queue[hot_queue_idx] = std::move(staging_queue.front());
          staging_queue.pop_front();
          std::get<0>(hot_queue[hot_queue_idx]).resume();
        } else {
          uint16_t workload_idx = fetch_workload();
          while (cold_txn_count == cold_queue_size && workload[workload_idx].name == "Read") {
            workload_idx = fetch_workload();
          }
          if (workload_idx == 1) {
            ++cold_txn_count;
          }
          next_free_tid = next_free_task_id_queue.front();
          next_free_task_id_queue.pop_front();
          task_workload_idxs[next_free_tid] = workload_idx;
          ASSERT(workload[workload_idx].task_fn);
          ermia::transaction *txn = nullptr;
          if (!ermia::config::index_probe_only) {
            if (workload[workload_idx].name == "OrderStatus" || workload[workload_idx].name == "StockLevel") {
              txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
            } else {
              txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[next_free_tid], &transactions[next_free_tid], next_free_tid);
            }
            txn->set_user_data(next_free_tid);
            txn->set_position(true, hot_queue_idx);
            ermia::TXN::xid_context *xc = txn->GetXIDContext();
            xc->begin_epoch = 0;
          } else {
            arenas[next_free_tid].reset();
          }
          hot_queue[hot_queue_idx] = std::make_tuple(workload[workload_idx].task_fn(this, txn, next_free_tid), txn, next_free_tid);
          ts[next_free_tid].lap();
          std::get<0>(hot_queue[hot_queue_idx]).start();
        }

        ++hot_txn_count;
      } else {
        std::get<0>(hot_queue[hot_queue_idx]).resume();
      }

      hot_queue_idx = (hot_queue_idx + 1) & (hot_queue_size - 1);
    }
    ermia::MM::epoch_exit(0, begin_epoch);
  }
};

ermia::coro::task<rc_t> tpcc_oltpim_worker::txn_new_order(ermia::transaction *txn, uint32_t idx) {
  uint _home_warehouse_id = 0;
  if (likely(FLAGS_tpcc_coro_local_wh)) {
    _home_warehouse_id = home_warehouse_id + idx;
  } else {
    _home_warehouse_id = home_warehouse_id;
  }
  const uint warehouse_id = pick_wh(r, _home_warehouse_id);
  const uint districtID = RandomNumber(r, 1, 10);
  const uint customerID = GetCustomerId(r);
  const uint numItems = RandomNumber(r, 5, 15);
  uint itemIDs[15], supplierWarehouseIDs[15], orderQuantities[15];
  bool allLocal = true;
  for (uint i = 0; i < numItems; i++) {
    itemIDs[i] = GetItemId(r);
    if (likely(FLAGS_tpcc_disable_xpartition_txn || NumWarehouses() == 1 ||
               RandomNumber(r, 1, 100) > FLAGS_tpcc_new_order_remote_item_pct)) {
      supplierWarehouseIDs[i] = warehouse_id;
    } else {
      do {
        supplierWarehouseIDs[i] = RandomNumber(r, 1, NumWarehouses());
      } while (supplierWarehouseIDs[i] == warehouse_id);
      allLocal = false;
    }
    orderQuantities[i] = RandomNumber(r, 1, 10);
  }
  ASSERT(!FLAGS_tpcc_disable_xpartition_txn || allLocal);

  // XXX(stephentu): implement rollback
  //
  // worst case txn profile:
  //   1 customer get
  //   1 warehouse get
  //   1 district get
  //   1 new_order insert
  //   1 district put
  //   1 oorder insert
  //   1 oorder_cid_idx insert
  //   15 times:
  //      1 item get
  //      1 stock get
  //      1 stock put
  //      1 order_line insert
  //
  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 0
  //   max_read_set_size : 15
  //   max_write_set_size : 15
  //   num_txn_contexts : 9

  // TODO: fix epoch
  txn->GetXIDContext()->begin_epoch = 0;

  rc_t rc = rc_t{RC_INVALID};
  ermia::varstr valptr;

  const customer::key k_c(warehouse_id, districtID, customerID);
  const uint64_t pk_c = tpcc_key64::customer(k_c);
  customer::value v_c_temp;

  rc = co_await tbl_customer(warehouse_id)->pim_GetRecord(txn, pk_c, valptr);
  TryVerifyRelaxedCoro(rc);

  const customer::value *v_c = Decode(valptr, v_c_temp);

#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, v_c);
#endif

  const warehouse::key k_w(warehouse_id);
  const uint64_t pk_w = tpcc_key64::warehouse(k_w);
  warehouse::value v_w_temp;

  rc = co_await tbl_warehouse(warehouse_id)->pim_GetRecord(txn, pk_w, valptr);
  TryVerifyRelaxedCoro(rc);

  const warehouse::value *v_w = Decode(valptr, v_w_temp);
#ifndef NDEBUG
  checker::SanityCheckWarehouse(&k_w, v_w);
#endif

  const district::key k_d(warehouse_id, districtID);
  const uint64_t pk_d = tpcc_key64::district(k_d);
  district::value v_d_temp;

  rc = co_await tbl_district(warehouse_id)->pim_GetRecord(txn, pk_d, valptr);
  TryVerifyRelaxedCoro(rc);

  const district::value *v_d = Decode(valptr, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  const uint64_t my_next_o_id =
      FLAGS_tpcc_new_order_fast_id_gen ? FastNewOrderIdGen(warehouse_id, districtID)
                              : v_d->d_next_o_id;

  const new_order::key k_no(warehouse_id, districtID, my_next_o_id);
  const uint64_t pk_no = tpcc_key64::new_order(k_no);
  const new_order::value v_no;
  const size_t new_order_sz = Size(v_no);
  rc = co_await tbl_new_order(warehouse_id)
           ->pim_InsertRecord(txn, pk_no,
                          Encode(str(arenas[idx], new_order_sz), v_no));
  TryCatchOltpim(rc);

  if (!FLAGS_tpcc_new_order_fast_id_gen) {
    district::value v_d_new(*v_d);
    v_d_new.d_next_o_id++;
    rc = co_await tbl_district(warehouse_id)
             ->pim_UpdateRecord(txn, pk_d,
                            Encode(str(arenas[idx], Size(v_d_new)), v_d_new));
    TryCatchOltpim(rc);
  }

  const oorder::key k_oo(warehouse_id, districtID, k_no.no_o_id);
  const uint64_t pk_oo = tpcc_key64::oorder(k_oo);
  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0;  // seems to be ignored
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = GetCurrentTimeMillis();

  const size_t oorder_sz = Size(v_oo);
  uint64_t v_oo_oid = 0;  // Get the OID and put it in oorder_c_id_idx later
  rc = co_await tbl_oorder(warehouse_id)
           ->pim_InsertRecord(txn, pk_oo,
                          Encode(str(arenas[idx], oorder_sz), v_oo), &v_oo_oid);
  TryCatchOltpim(rc);

  const oorder_c_id_idx::key k_oo_idx(warehouse_id, districtID, customerID, k_no.no_o_id);
  const uint64_t pk_oo_idx = tpcc_key64::oorder_c_id_idx(k_oo_idx);
  rc = co_await tbl_oorder_c_id_idx(warehouse_id)
           ->pim_InsertOID(txn, pk_oo_idx, v_oo_oid);
  TryCatchOltpim(rc);


  for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
    const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = itemIDs[ol_number - 1];
    const uint ol_quantity = orderQuantities[ol_number - 1];

    const item::key k_i(ol_i_id);
    const uint64_t pk_i = tpcc_key64::item(k_i);
    item::value v_i_temp;

    rc = co_await tbl_item(1)->pim_GetRecord(txn, pk_i, valptr);
    TryVerifyRelaxedCoro(rc);

    const item::value *v_i = Decode(valptr, v_i_temp);
#ifndef NDEBUG
    checker::SanityCheckItem(&k_i, v_i);
#endif

    const stock::key k_s(ol_supply_w_id, ol_i_id);
    const uint64_t pk_s = tpcc_key64::stock(k_s);
    stock::value v_s_temp;

    rc = co_await tbl_stock(ol_supply_w_id)->pim_GetRecord(txn, pk_s, valptr);
    TryVerifyRelaxedCoro(rc);
    const stock::value *v_s = Decode(valptr, v_s_temp);

#ifndef NDEBUG
    checker::SanityCheckStock(&k_s);
#endif

    stock::value v_s_new(*v_s);
    if (v_s_new.s_quantity - ol_quantity >= 10)
      v_s_new.s_quantity -= ol_quantity;
    else
      v_s_new.s_quantity += -int32_t(ol_quantity) + 91;
    v_s_new.s_ytd += ol_quantity;
    v_s_new.s_remote_cnt += (ol_supply_w_id == warehouse_id) ? 0 : 1;

    rc = co_await tbl_stock(ol_supply_w_id)
              ->pim_UpdateRecord(txn, pk_s,
                            Encode(str(arenas[idx], Size(v_s_new)), v_s_new));
    TryCatchOltpim(rc);

    const order_line::key k_ol(warehouse_id, districtID, k_no.no_o_id,
                              ol_number);
    const uint64_t pk_ol = tpcc_key64::order_line(k_ol);
    order_line::value v_ol;
    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0;  // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * v_i->i_price;
    v_ol.ol_supply_w_id = int32_t(ol_supply_w_id);
    v_ol.ol_quantity = int8_t(ol_quantity);

    const size_t order_line_sz = Size(v_ol);
    rc = co_await tbl_order_line(warehouse_id)
              ->pim_InsertRecord(txn, pk_ol,
                            Encode(str(arenas[idx], order_line_sz), v_ol));
    TryCatchOltpim(rc);
  }

#ifndef CORO_BATCH_COMMIT
  rc = co_await db->Commit(txn);
  TryCatchOltpim(rc);
#endif
  co_return {RC_TRUE};
}  // new-order

ermia::coro::task<rc_t> tpcc_oltpim_worker::txn_payment(ermia::transaction *txn, uint32_t idx) {
  uint _home_warehouse_id = 0;
  if (likely(FLAGS_tpcc_coro_local_wh)) {
    _home_warehouse_id = home_warehouse_id + idx;
  } else {
    _home_warehouse_id = home_warehouse_id;
  }
  const uint warehouse_id = pick_wh(r, _home_warehouse_id);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  uint customerDistrictID, customerWarehouseID;
  if (likely(FLAGS_tpcc_disable_xpartition_txn || NumWarehouses() == 1 ||
             RandomNumber(r, 1, 100) <= 85)) {
    customerDistrictID = districtID;
    customerWarehouseID = warehouse_id;
  } else {
    customerDistrictID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
    do {
      customerWarehouseID = RandomNumber(r, 1, NumWarehouses());
    } while (customerWarehouseID == warehouse_id);
  }
  const float paymentAmount = (float)(RandomNumber(r, 100, 500000) / 100.0);
  const uint32_t ts = GetCurrentTimeMillis();
  ASSERT(!FLAGS_tpcc_disable_xpartition_txn || customerWarehouseID == warehouse_id);

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 10
  //   max_read_set_size : 71
  //   max_write_set_size : 1
  //   num_txn_contexts : 5

  // TODO: fix epoch
  txn->GetXIDContext()->begin_epoch = 0;

  const warehouse::key k_w(warehouse_id);
  const uint64_t pk_w = tpcc_key64::warehouse(k_w);
  warehouse::value v_w_temp;
  ermia::varstr valptr;

  rc_t rc = rc_t{RC_INVALID};

  rc = co_await tbl_warehouse(warehouse_id)->pim_GetRecord(txn, pk_w, valptr);
  TryVerifyRelaxedCoro(rc);
  const warehouse::value *v_w = Decode(valptr, v_w_temp);

#ifndef NDEBUG
  checker::SanityCheckWarehouse(&k_w, v_w);
#endif

  warehouse::value v_w_new(*v_w);
  v_w_new.w_ytd += paymentAmount;
  rc = co_await tbl_warehouse(warehouse_id)
           ->pim_UpdateRecord(txn, pk_w,
                          Encode(str(arenas[idx], Size(v_w_new)), v_w_new));
  TryCatchOltpim(rc);

  const district::key k_d(warehouse_id, districtID);
  const uint64_t pk_d = tpcc_key64::district(k_d);
  district::value v_d_temp;

  rc = co_await tbl_district(warehouse_id)->pim_GetRecord(txn, pk_d, valptr);
  TryVerifyRelaxedCoro(rc);

  valptr.prefetch();
  co_await suspend_always{};

  const district::value *v_d = Decode(valptr, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  district::value v_d_new(*v_d);
  v_d_new.d_ytd += paymentAmount;
  rc = co_await tbl_district(warehouse_id)
           ->pim_UpdateRecord(txn, pk_d,
                          Encode(str(arenas[idx], Size(v_d_new)), v_d_new));
  TryCatchOltpim(rc);

  customer::key k_c;
  uint64_t pk_c;
  customer::value v_c;
  if (RandomNumber(r, 1, 100) <= 60) {
    // cust by name
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    memset(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, r);

    customer_name_idx::key k_c_idx;
    k_c_idx.c_w_id = customerWarehouseID;
    k_c_idx.c_d_id = customerDistrictID;
    k_c_idx.c_last.assign((const char *)lastname_buf, 16);
    const uint64_t pk_c_idx_0 = tpcc_key64::customer_name_idx(k_c_idx, true, false);
    const uint64_t pk_c_idx_1 = tpcc_key64::customer_name_idx(k_c_idx, false, true);

    pim_static_limit_callback<NMaxCustomerIdxScanElems,false> c;
    rc = co_await tbl_customer_name_idx(customerWarehouseID)
             ->pim_Scan(txn, pk_c_idx_0, pk_c_idx_1, c, NMaxCustomerIdxScanElems);
    TryCatchOltpim(rc);

    ALWAYS_ASSERT(c.size() > 0);
    //ASSERT(c.size() < NMaxCustomerIdxScanElems);
    int index = c.size() / 2;
    if (c.size() % 2 == 0) index--;

    Decode(c.values[index], v_c);
    k_c.c_w_id = customerWarehouseID;
    k_c.c_d_id = customerDistrictID;
    k_c.c_id = v_c.c_id;
    pk_c = tpcc_key64::customer(k_c);
  } else {
    // cust by ID
    const uint customerID = GetCustomerId(r);
    k_c.c_w_id = customerWarehouseID;
    k_c.c_d_id = customerDistrictID;
    k_c.c_id = customerID;
    pk_c = tpcc_key64::customer(k_c);
    rc = co_await tbl_customer(customerWarehouseID)->pim_GetRecord(txn, pk_c, valptr);
    TryVerifyRelaxedCoro(rc);
    Decode(valptr, v_c);
  }

#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, &v_c);
#endif
  customer::value v_c_new(v_c);

  v_c_new.c_balance -= paymentAmount;
  v_c_new.c_ytd_payment += paymentAmount;
  v_c_new.c_payment_cnt++;
  if (strncmp(v_c.c_credit.data(), "BC", 2) == 0) {
    char buf[501];
    int n = snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s", k_c.c_id,
                     k_c.c_d_id, k_c.c_w_id, districtID, warehouse_id,
                     paymentAmount, v_c.c_data.c_str());
    v_c_new.c_data.resize_junk(
        std::min(static_cast<size_t>(n), v_c_new.c_data.max_size()));
    memcpy((void *)v_c_new.c_data.data(), &buf[0], v_c_new.c_data.size());
  }

  rc = co_await tbl_customer(customerWarehouseID)
           ->pim_UpdateRecord(txn, pk_c,
                          Encode(str(arenas[idx], Size(v_c_new)), v_c_new));
  TryCatchOltpim(rc);

  const history::key k_h(k_c.c_d_id, k_c.c_w_id, k_c.c_id, districtID,
                         warehouse_id, ts);
  const uint64_t pk_h = tpcc_key64::history(k_h);
  history::value v_h;
  v_h.h_amount = paymentAmount;
  v_h.h_data.resize_junk(v_h.h_data.max_size());
  int n = snprintf((char *)v_h.h_data.data(), v_h.h_data.max_size() + 1,
                   "%.10s    %.10s", v_w->w_name.c_str(), v_d->d_name.c_str());
  v_h.h_data.resize_junk(
      std::min(static_cast<size_t>(n), v_h.h_data.max_size()));

  // XXX(tzwang): 1.85->1.86mtps (no log memcpy)
  rc = co_await tbl_history(warehouse_id)
           ->pim_InsertRecord(txn, pk_h,
                          Encode(str(arenas[idx], Size(v_h)), v_h));
  TryCatchOltpim(rc);

#ifndef CORO_BATCH_COMMIT
  rc = co_await db->Commit(txn);
  TryCatchOltpim(rc);
#endif
  co_return {RC_TRUE};
}  // payment

ermia::coro::task<rc_t> tpcc_oltpim_worker::txn_delivery(ermia::transaction *txn, uint32_t idx) {
  ermia::TXN::xid_context *xc = txn->GetXIDContext();
  // TODO: fix epoch
  xc->begin_epoch = 0;
  rc_t rc = rc_t{RC_INVALID};

  uint _home_warehouse_id = 0;
  if (likely(FLAGS_tpcc_coro_local_wh)) {
    _home_warehouse_id = home_warehouse_id + idx;
  } else {
    _home_warehouse_id = home_warehouse_id;
  }
  const uint warehouse_id = pick_wh(r, _home_warehouse_id);
  const uint o_carrier_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  const uint32_t ts = GetCurrentTimeMillis();

  // worst case txn profile:
  //   10 times:
  //     1 new_order scan node
  //     1 oorder get
  //     2 order_line scan nodes
  //     15 order_line puts
  //     1 new_order remove
  //     1 oorder put
  //     1 customer get
  //     1 customer put
  //
  // output from counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 21
  //   max_read_set_size : 133
  //   max_write_set_size : 133
  //   num_txn_contexts : 4
  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    const new_order::key k_no_0(warehouse_id, d, last_no_o_ids[d - 1]);
    const uint64_t pk_no_0 = tpcc_key64::new_order(k_no_0);
    const new_order::key k_no_1(warehouse_id, d,
                                0xFFFFFUL); // 20bit encoded
    const uint64_t pk_no_1 = tpcc_key64::new_order(k_no_1);
    pim_static_limit_callback<1,true> new_order_c;
    {
      rc = co_await tbl_new_order(warehouse_id)
               ->pim_Scan(txn, pk_no_0, pk_no_1, new_order_c, 1);
      TryCatchOltpim(rc);
    }
    if (unlikely(new_order_c.size() == 0)) continue;

    new_order::value v_no;
    Decode(new_order_c.values[0], v_no);
    last_no_o_ids[d - 1] = v_no.no_o_id + 1;  // XXX: update last seen

    const oorder::key k_oo(warehouse_id, d, v_no.no_o_id);
    const uint64_t pk_oo = tpcc_key64::oorder(k_oo);
    // even if we read the new order entry, there's no guarantee
    // we will read the oorder entry: in this case the txn will abort,
    // but we're simply bailing out early
    oorder::value v_oo_temp;
    ermia::varstr valptr;
    rc = co_await tbl_oorder(warehouse_id)->pim_GetRecord(txn, pk_oo, valptr);
    TryCatchOltpim(rc);

    valptr.prefetch();
    co_await suspend_always{};
    const oorder::value *v_oo = Decode(valptr, v_oo_temp);
#ifndef NDEBUG
    checker::SanityCheckOOrder(&k_oo, v_oo);
#endif

    pim_static_limit_callback<15,true> c;  // never more than 15 order_lines per order
    const order_line::key k_oo_0(warehouse_id, d, v_no.no_o_id, 0);
    const uint64_t pk_oo_0 = tpcc_key64::order_line(k_oo_0);
    const order_line::key k_oo_1(warehouse_id, d, v_no.no_o_id,
                                 0xFUL); // 4bit encoded
    const uint64_t pk_oo_1 = tpcc_key64::order_line(k_oo_1);

    // XXX(stephentu): mutable scans would help here
    rc = co_await tbl_order_line(warehouse_id)
             ->pim_Scan(txn, pk_oo_0, pk_oo_1, c, 15);
    TryCatchOltpim(rc);

    float sum = 0.0;
    for (size_t i = 0; i < c.size(); i++) {
      order_line::value v_ol;
      Decode(c.values[i], v_ol);

      const order_line::key k_ol(v_ol.ol_w_id, v_ol.ol_d_id, v_ol.ol_o_id, v_ol.ol_number);
      const uint64_t pk_ol = tpcc_key64::order_line(k_ol);
#ifndef NDEBUG
      checker::SanityCheckOrderLine(&k_ol, &v_ol);
#endif

      sum += v_ol.ol_amount;
      order_line::value v_ol_new(v_ol);
      v_ol_new.ol_delivery_d = ts;
      // TODO callback store uint64 pk instead of varstr and use directly
      rc = co_await tbl_order_line(warehouse_id)
                ->pim_UpdateRecord(txn, pk_ol,
                      Encode(str(arenas[idx], Size(v_ol_new)), v_ol_new));
      TryCatchOltpim(rc);
    }

    // delete new order
    const new_order::key k_no(v_no.no_w_id, v_no.no_d_id, v_no.no_o_id);
    const uint64_t pk_no = tpcc_key64::new_order(k_no);
    rc = co_await tbl_new_order(warehouse_id)
             ->pim_RemoveRecord(txn, pk_no);
    TryCatchOltpim(rc);

    // update oorder
    oorder::value v_oo_new(*v_oo);
    v_oo_new.o_carrier_id = o_carrier_id;
    rc = co_await tbl_oorder(warehouse_id)
            ->pim_UpdateRecord(txn, pk_oo,
                           Encode(str(arenas[idx], Size(v_oo_new)), v_oo_new));

    TryCatchOltpim(rc);

    const uint c_id = v_oo->o_c_id;
    const float ol_total = sum;

    // update customer
    const customer::key k_c(warehouse_id, d, c_id);
    const uint64_t pk_c = tpcc_key64::customer(k_c);
    customer::value v_c_temp;

    rc = co_await tbl_customer(warehouse_id)->pim_GetRecord(txn, pk_c, valptr);
    TryVerifyRelaxedCoro(rc);

    // XXX(tzwang): prefetch valptr here doesn't help (100% delivery)
    const customer::value *v_c = Decode(valptr, v_c_temp);
    customer::value v_c_new(*v_c);
    v_c_new.c_balance += ol_total;
    rc = co_await tbl_customer(warehouse_id)
             ->pim_UpdateRecord(txn, pk_c,
                            Encode(str(arenas[idx], Size(v_c_new)), v_c_new));
    TryCatchOltpim(rc);
  }

#ifndef CORO_BATCH_COMMIT
  rc = co_await db->Commit(txn);
  TryCatchOltpim(rc);
#endif

  co_return {RC_TRUE};
}  // delivery

ermia::coro::task<rc_t> tpcc_oltpim_worker::txn_order_status(ermia::transaction *txn, uint32_t idx) {
  // NB: since txn_order_status() is a RO txn, we assume that
  // locking is un-necessary (since we can just read from some old snapshot)
  ermia::TXN::xid_context *xc = txn->GetXIDContext();
  // TODO: fix epoch
  xc->begin_epoch = 0;
  rc_t rc = rc_t{RC_INVALID};

  uint _home_warehouse_id = 0;
  if (likely(FLAGS_tpcc_coro_local_wh)) {
    _home_warehouse_id = home_warehouse_id + idx;
  } else {
    _home_warehouse_id = home_warehouse_id;
  }
  const uint warehouse_id = pick_wh(r, _home_warehouse_id);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 13
  //   max_read_set_size : 81
  //   max_write_set_size : 0
  //   num_txn_contexts : 4
  customer::key k_c;
  uint64_t pk_c;
  customer::value v_c;
  ermia::varstr valptr;
  if (RandomNumber(r, 1, 100) <= 60) {
    // cust by name
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    memset(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, r);

    static const std::string zeros(16, 0);
    static const std::string ones(16, (char)255);

    customer_name_idx::key k_c_idx_0;
    k_c_idx_0.c_w_id = warehouse_id;
    k_c_idx_0.c_d_id = districtID;
    k_c_idx_0.c_last.assign((const char *)lastname_buf, 16);
    k_c_idx_0.c_first.assign(zeros);
    const uint64_t pk_c_idx_0 = tpcc_key64::customer_name_idx(k_c_idx_0, true, false);

    customer_name_idx::key k_c_idx_1;
    k_c_idx_1.c_w_id = warehouse_id;
    k_c_idx_1.c_d_id = districtID;
    k_c_idx_1.c_last.assign((const char *)lastname_buf, 16);
    k_c_idx_1.c_first.assign(ones);
    const uint64_t pk_c_idx_1 = tpcc_key64::customer_name_idx(k_c_idx_1, false, true);

    pim_static_limit_callback<NMaxCustomerIdxScanElems,false> c;  // probably a safe bet for now
    rc = co_await tbl_customer_name_idx(warehouse_id)
             ->pim_Scan(txn, pk_c_idx_0, pk_c_idx_1, c, NMaxCustomerIdxScanElems);
    TryCatchOltpim(rc);
    ALWAYS_ASSERT(c.size() > 0);
    //ASSERT(c.size() < NMaxCustomerIdxScanElems);
    int index = c.size() / 2;
    if (c.size() % 2 == 0) index--;

    Decode(c.values[index], v_c);
    k_c.c_w_id = warehouse_id;
    k_c.c_d_id = districtID;
    k_c.c_id = v_c.c_id;
    pk_c = tpcc_key64::customer(k_c);
  } else {
    // cust by ID
    const uint customerID = GetCustomerId(r);
    k_c.c_w_id = warehouse_id;
    k_c.c_d_id = districtID;
    k_c.c_id = customerID;
    pk_c = tpcc_key64::customer(k_c);

    rc = co_await tbl_customer(warehouse_id)->pim_GetRecord(txn, pk_c, valptr);
    TryVerifyRelaxedCoro(rc);
    valptr.prefetch();
    co_await suspend_always{};
    Decode(valptr, v_c);
  }
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, &v_c);
#endif

  oorder_c_id_idx::value sv;
  // oorder_c_id_idx's o_id is stored in reversed order in tpcc-key64.h
  // so one scan is enough
  pim_static_limit_callback<1,false> c_oorder;
  const oorder_c_id_idx::key k_oo_idx_0(warehouse_id, districtID, k_c.c_id,
                                        0);
  const uint64_t pk_oo_idx_0 = tpcc_key64::oorder_c_id_idx(k_oo_idx_0);
  const oorder_c_id_idx::key k_oo_idx_1(warehouse_id, districtID, k_c.c_id,
                                        0xFFFFFUL); // 20bit o_id
  const uint64_t pk_oo_idx_1 = tpcc_key64::oorder_c_id_idx(k_oo_idx_1);
  {
    // o_id stored in reverse order: search from pk_1 to pk_0
    rc = co_await tbl_oorder_c_id_idx(warehouse_id)
            ->pim_Scan(txn, pk_oo_idx_1, pk_oo_idx_0, c_oorder, 1);
    TryCatchOltpim(rc);
  }
  ALWAYS_ASSERT(c_oorder.size());

  oorder::value v_oo;
  Decode(c_oorder.values[0], v_oo);
  const uint o_id = v_oo.o_id;

  pim_static_limit_callback<15,true> c_order_line;
  const order_line::key k_ol_0(warehouse_id, districtID, o_id, 0);
  const uint64_t pk_ol_0 = tpcc_key64::order_line(k_ol_0);
  const order_line::key k_ol_1(warehouse_id, districtID, o_id,
                               0xFUL);
  const uint64_t pk_ol_1 = tpcc_key64::order_line(k_ol_1);
  rc = co_await tbl_order_line(warehouse_id)
           ->pim_Scan(txn, pk_ol_0, pk_ol_1, c_order_line, 15);
  TryCatchOltpim(rc);
  ALWAYS_ASSERT(c_order_line.size() >= 5 && c_order_line.size() <= 15);

#ifndef CORO_BATCH_COMMIT
  rc = co_await db->Commit(txn);
  TryCatchOltpim(rc);
#endif

  co_return {RC_TRUE};
}  // order-status

ermia::coro::task<rc_t> tpcc_oltpim_worker::txn_stock_level(ermia::transaction *txn, uint32_t idx) {
  // NB: since txn_stock_level() is a RO txn, we assume that
  // locking is un-necessary (since we can just read from some old snapshot)
  ermia::TXN::xid_context *xc = txn->GetXIDContext();
  // TODO: fix epoch
  xc->begin_epoch = 0;
  rc_t rc = rc_t{RC_INVALID};

  uint _home_warehouse_id = 0;
  if (likely(FLAGS_tpcc_coro_local_wh)) {
    _home_warehouse_id = home_warehouse_id + idx;
  } else {
    _home_warehouse_id = home_warehouse_id;
  }
  const uint warehouse_id = pick_wh(r, _home_warehouse_id);
  const uint threshold = RandomNumber(r, 10, 20);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 19
  //   max_read_set_size : 241
  //   max_write_set_size : 0
  //   n_node_scan_large_instances : 1
  //   n_read_set_large_instances : 2
  //   num_txn_contexts : 3
  const district::key k_d(warehouse_id, districtID);
  const uint64_t pk_d = tpcc_key64::district(k_d);
  district::value v_d_temp;
  ermia::varstr valptr;

  rc = co_await tbl_district(warehouse_id)->pim_GetRecord(txn, pk_d, valptr);
  TryVerifyRelaxedCoro(rc);

  const district::value *v_d = Decode(valptr, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  const uint64_t cur_next_o_id =
      FLAGS_tpcc_new_order_fast_id_gen
          ? NewOrderIdHolder(warehouse_id, districtID)
                .load(std::memory_order_acquire)
          : v_d->d_next_o_id;

  // manual joins are fun!
  pim_static_limit_callback<15,true,20> c;
  const int32_t lower = cur_next_o_id >= 20 ? (cur_next_o_id - 20) : 0;
  const order_line::key k_ol_0(warehouse_id, districtID, lower, 0);
  const uint64_t pk_ol_0 = tpcc_key64::order_line(k_ol_0);
  const order_line::key k_ol_1(warehouse_id, districtID, cur_next_o_id - 1, 0xFUL);
  const uint64_t pk_ol_1 = tpcc_key64::order_line(k_ol_1);
  {
    rc = co_await tbl_order_line(warehouse_id)
             ->pim_Scan(txn, pk_ol_0, pk_ol_1, c, 15);
    TryCatchOltpim(rc);
  }

  std::unordered_map<uint, bool> s_i_ids_distinct;
  for (size_t i = 0; i < c.size(); ++i) {
    order_line::value v_ol;
    Decode(c.values[i], v_ol);
    const stock::key k_s(warehouse_id, v_ol.ol_i_id);
    const uint64_t pk_s = tpcc_key64::stock(k_s);
    stock::value v_s;
    ASSERT(v_ol.ol_i_id >= 1 && v_ol.ol_i_id <= NumItems());

    rc = co_await tbl_stock(warehouse_id)->pim_GetRecord(txn, pk_s, valptr);
    TryVerifyRelaxedCoro(rc);

    const uint8_t *ptr = (const uint8_t *)valptr.data();
    int16_t i16tmp;
    ptr = serializer<int16_t, true>::read(ptr, &i16tmp);
    if (i16tmp < int(threshold)) s_i_ids_distinct[v_ol.ol_i_id] = 1;
  }
  // NB(stephentu): s_i_ids_distinct.size() is the computed result of this txn

#ifndef CORO_BATCH_COMMIT
  rc = co_await db->Commit(txn);
  TryCatchOltpim(rc);
#endif

  co_return {RC_TRUE};
}  // stock-level

ermia::coro::task<rc_t> tpcc_oltpim_worker::txn_credit_check(ermia::transaction *txn, uint32_t idx) {
  ALWAYS_ASSERT(false);
  co_return {RC_TRUE};
}  // credit-check

ermia::coro::task<rc_t> tpcc_oltpim_worker::txn_query2(ermia::transaction *txn, uint32_t idx) {
  ALWAYS_ASSERT(false);
  co_return {RC_TRUE};
}

ermia::coro::task<rc_t> tpcc_oltpim_worker::txn_microbench_random(ermia::transaction *txn, uint32_t idx) {
  ALWAYS_ASSERT(false);
  co_return {RC_TRUE};
}

bench_worker::workload_desc_vec tpcc_oltpim_worker::get_workload() const {
  workload_desc_vec w;
  // numbers from sigmod.csail.mit.edu:
  // w.push_back(workload_desc("NewOrder", 1.0, TxnNewOrder)); // ~10k ops/sec
  // w.push_back(workload_desc("Payment", 1.0, TxnPayment)); // ~32k ops/sec
  // w.push_back(workload_desc("Delivery", 1.0, TxnDelivery)); // ~104k
  // ops/sec
  // w.push_back(workload_desc("OrderStatus", 1.0, TxnOrderStatus)); // ~33k
  // ops/sec
  // w.push_back(workload_desc("StockLevel", 1.0, TxnStockLevel)); // ~2k
  // ops/sec
  unsigned m = 0;
  for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
    m += g_txn_workload_mix[i];
  ALWAYS_ASSERT(m == 100);
  if (g_txn_workload_mix[0])
    w.push_back(workload_desc(
        "NewOrder", double(g_txn_workload_mix[0]) / 100.0, nullptr, nullptr, TxnNewOrder));
  if (g_txn_workload_mix[1])
    w.push_back(workload_desc(
        "Payment", double(g_txn_workload_mix[1]) / 100.0, nullptr, nullptr, TxnPayment));
  if (g_txn_workload_mix[2])
    w.push_back(workload_desc(
        "CreditCheck",double(g_txn_workload_mix[2]) / 100.0, nullptr, nullptr, TxnCreditCheck));
  if (g_txn_workload_mix[3])
    w.push_back(workload_desc(
        "Delivery", double(g_txn_workload_mix[3]) / 100.0, nullptr, nullptr, TxnDelivery));
  if (g_txn_workload_mix[4])
    w.push_back(workload_desc(
        "OrderStatus", double(g_txn_workload_mix[4]) / 100.0, nullptr, nullptr, TxnOrderStatus));
  if (g_txn_workload_mix[5])
    w.push_back(workload_desc(
        "StockLevel", double(g_txn_workload_mix[5]) / 100.0, nullptr, nullptr, TxnStockLevel));
  if (g_txn_workload_mix[6])
    w.push_back(workload_desc(
        "Query2", double(g_txn_workload_mix[6]) / 100.0, nullptr, nullptr, TxnQuery2));
  if (g_txn_workload_mix[7])
    w.push_back(workload_desc(
        "MicroBenchRandom", double(g_txn_workload_mix[7]) / 100.0, nullptr, nullptr, TxnMicroBenchRandom));
  return w;
}

bench_worker::workload_desc_vec tpcc_oltpim_worker::get_cold_workload() const {
  workload_desc_vec w;
  // numbers from sigmod.csail.mit.edu:
  // w.push_back(workload_desc("NewOrder", 1.0, TxnNewOrder)); // ~10k ops/sec
  // w.push_back(workload_desc("Payment", 1.0, TxnPayment)); // ~32k ops/sec
  // w.push_back(workload_desc("Delivery", 1.0, TxnDelivery)); // ~104k
  // ops/sec
  // w.push_back(workload_desc("OrderStatus", 1.0, TxnOrderStatus)); // ~33k
  // ops/sec
  // w.push_back(workload_desc("StockLevel", 1.0, TxnStockLevel)); // ~2k
  // ops/sec
  unsigned m = 0;
  for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
    m += g_txn_workload_mix[i];
  ALWAYS_ASSERT(m == 100);
  if (g_txn_workload_mix[0])
    w.push_back(workload_desc(
        "NewOrder", double(g_txn_workload_mix[0]) / 100.0, nullptr, nullptr, TxnNewOrder));
  if (g_txn_workload_mix[1])
    w.push_back(workload_desc(
        "Payment", double(g_txn_workload_mix[1]) / 100.0, nullptr, nullptr, TxnPayment));
  if (g_txn_workload_mix[2])
    w.push_back(workload_desc(
        "CreditCheck",double(g_txn_workload_mix[2]) / 100.0, nullptr, nullptr, TxnCreditCheck));
  if (g_txn_workload_mix[3])
    w.push_back(workload_desc(
        "Delivery", double(g_txn_workload_mix[3]) / 100.0, nullptr, nullptr, TxnDelivery));
  if (g_txn_workload_mix[4])
    w.push_back(workload_desc(
        "OrderStatus", double(g_txn_workload_mix[4]) / 100.0, nullptr, nullptr, TxnOrderStatus));
  if (g_txn_workload_mix[5])
    w.push_back(workload_desc(
        "StockLevel", double(g_txn_workload_mix[5]) / 100.0, nullptr, nullptr, TxnStockLevel));
  if (g_txn_workload_mix[6])
    w.push_back(workload_desc(
        "Query2", double(g_txn_workload_mix[6]) / 100.0, nullptr, nullptr, TxnQuery2));
  if (g_txn_workload_mix[7])
    w.push_back(workload_desc(
        "MicroBenchRandom", double(g_txn_workload_mix[7]) / 100.0, nullptr, nullptr, TxnMicroBenchRandom));
  return w;
}

// Essentially a coroutine scheduler that switches between active transactions
void tpcc_oltpim_worker::MyWork(char *) {
  // No replication support
  ALWAYS_ASSERT(is_worker);
  tlog = ermia::GetLog();
  workload = get_workload();
  txn_counts.resize(workload.size());
  _coro_batch_size = ermia::config::coro_batch_size;

  auto schedule_mode = ermia::config::coro_scheduler;
  LOG_IF(FATAL, ermia::config::io_threads > ermia::config::worker_threads) << "Not enough threads.";
  if (ermia::config::io_threads) {
    if (worker_id < ermia::config::io_threads) {
      workload = get_cold_workload();
      schedule_mode = ermia::config::coro_io_scheduler;
      _coro_batch_size = ermia::config::coro_io_batch_size;
    } else {
      workload = get_workload();
    }
  }
  if (schedule_mode == 0) {
    HybridBatch();
  } else if (schedule_mode == 1) {
    HybridVanillaPipeline();
  } else if (schedule_mode == 2) {
    HybridSingleQPipelineAC();
  } else if (schedule_mode == 3) {
    HybridDualQPipeline();
  } else if (schedule_mode == 4) {
    HybridDualQPipelineAC();
  } else {
    LOG(FATAL) << "\n-coro_scheduler=<0|1|2|3|4>"
                  "\n0: batch scheduler"
                  "\n1: vanilla single queue pipeline"
                  "\n2: single queue pipeline with admission control"
                  "\n3: dual-queue pipeline"
                  "\n4: dual-queue pipeline with admission control";
  }
}

void tpcc_oltpim_do_test(ermia::Engine *db) {
  ermia::config::read_txn_type = "tpcc-hybrid-coro";
  tpcc_parse_options();
  tpcc_bench_runner<tpcc_oltpim_worker> r(db);
  r.run();
}

int main(int argc, char **argv) {
  bench_main(argc, argv, tpcc_oltpim_do_test);
  return 0;
}

#endif // NOT NESTED_COROUTINE
