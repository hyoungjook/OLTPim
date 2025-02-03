#include <getopt.h>
#include <ostream>

#include <gflags/gflags.h>

#include "../../engine.h"
#include "../bench.h"
#include "ycsb.h"

DEFINE_uint64(ycsb_ops_per_tx, 10, "Number of operations to issue per transaction");
DEFINE_uint64(ycsb_ops_per_hot_tx, 10, "Number of operations to issue per hot transaction");
DEFINE_uint64(ycsb_ins_per_tx, 10, "Number of inserts per transaction");
DEFINE_uint64(ycsb_update_per_tx, 10, "Number of updates per transaction");
DEFINE_double(ycsb_hot_tx_percent, 1, "Percentage of hot transactions in the whole workload");
DEFINE_double(ycsb_remote_tx_percent, 0, "Percentage of remote transactions in the whole workload");
DEFINE_uint64(ycsb_cold_ops_per_tx, 0, "Cold operations to issue per transaction");
DEFINE_uint64(ycsb_rmw_additional_reads, 0, "Additional reads to issue in the RMW transaction");
DEFINE_uint64(ycsb_hot_table_size, 100000, "In-memory (hot) table size");
DEFINE_uint64(ycsb_cold_table_size, 0, "In-storage (cold) table size");
DEFINE_uint64(ycsb_max_scan_size, 10, "Maximum scan size");
DEFINE_uint64(ycsb_latency, 0, "Simulated data access latency");

DEFINE_double(ycsb_zipfian_theta, 0.99, "Zipfian theta (for hot table only)");

DEFINE_string(ycsb_workload, "C", "Workload: A - H");
DEFINE_string(ycsb_hot_table_rng, "uniform", "RNG to use to issue ops for the hot table; uniform or zipfian.");
DEFINE_string(ycsb_read_tx_type, "sequential", "Type of read transaction: sequential or multiget-amac.");
DEFINE_bool(ycsb_numa_local, false, "Use NUMA-local tables and transactions.");

bool g_hot_table_zipfian = false;

// TODO: support scan_min length, current zipfain rng does not support min bound.
const int g_scan_min_length = 1;
int g_scan_length_zipfain_rng = 0;
double g_scan_length_zipfain_theta = 0.99;

// A spin barrier for all loaders to finish
std::atomic<uint32_t> loaders_barrier(0);
std::atomic<bool> all_flushed(false);

ReadTransactionType g_read_txn_type = ReadTransactionType::Sequential;

// { insert, read, update, scan, rmw }
YcsbWorkload YcsbWorkloadA('A', 0, 50U, 100U, 0, 0);  // Workload A - 50% read, 50% update
YcsbWorkload YcsbWorkloadB('B', 0, 95U, 100U, 0, 0);  // Workload B - 95% read, 5% update
YcsbWorkload YcsbWorkloadC('C', 0, 100U, 0, 0, 0);  // Workload C - 100% read
YcsbWorkload YcsbWorkloadD('D', 5U, 100U, 0, 0, 0);  // Workload D - 95% read, 5% insert
YcsbWorkload YcsbWorkloadE('E', 5U, 0, 0, 100U, 0);  // Workload E - 5% insert, 95% scan

// Combine reps_per_tx and rmw_additional_reads to have "10R+10RMW" style
// transactions.
YcsbWorkload YcsbWorkloadF('F', 0, 0, 0, 0, 100U);  // Workload F - 100% RMW

// Extra workloads (not in spec)
YcsbWorkload YcsbWorkloadG('G', 0, 0, 5U, 100U, 0);  // Workload G - 5% update, 95% scan
YcsbWorkload YcsbWorkloadI1('i', 25U, 100U, 0, 0, 0);  // Workload I1 - 25% insert, 75% read
YcsbWorkload YcsbWorkloadI2('j', 50U, 100U, 0, 0, 0);  // Workload I2 - 50% insert, 50% read
YcsbWorkload YcsbWorkloadI3('k', 75U, 100U, 0, 0, 0);  // Workload I3 - 75% insert, 25% read
YcsbWorkload YcsbWorkloadI4('I', 100U, 0, 0, 0, 0);  // Workload I4 - 100% insert
YcsbWorkload YcsbWorkloadS('S', 0, 0, 0, 100U, 0);  // Workload S - 100% scan

YcsbWorkload ycsb_workload = YcsbWorkloadC;

void ycsb_create_db(ermia::Engine *db) {
  ermia::thread::Thread *thread = ermia::thread::GetThread(ermia::thread::CoreType::PHYSICAL);
  ALWAYS_ASSERT(thread);

  auto create_table = [=](char *) {
    if (!FLAGS_ycsb_numa_local) {
      db->CreateTable("USERTABLE");
      db->CreateMasstreePrimaryIndex("USERTABLE", std::string("USERTABLE"));
    }
    else {
      for (int i = 0; i < ermia::config::numa_nodes; ++i) {
        std::string name = std::string("USERTABLE") + std::to_string(i);
        db->CreateTable(name.c_str());
        db->CreateMasstreePrimaryIndex(name.c_str(), name);
      }
    }
  };

  thread->StartTask(create_table);
  thread->Join();
  ermia::thread::PutThread(thread);
}

void ycsb_table_loader::load() {
  uint32_t nloaders = std::thread::hardware_concurrency();
  ALWAYS_ASSERT(loader_id < nloaders);
  if (!FLAGS_ycsb_numa_local) {
    LOG(INFO) << "Loading user table, " << FLAGS_ycsb_hot_table_size << " hot records, " << FLAGS_ycsb_cold_table_size << " cold records.";
    uint64_t hot_to_insert = FLAGS_ycsb_hot_table_size / nloaders;
    uint64_t cold_to_insert = FLAGS_ycsb_cold_table_size / nloaders;
    uint64_t hot_start_key = loader_id * hot_to_insert;
    uint64_t cold_start_key = FLAGS_ycsb_hot_table_size + loader_id * cold_to_insert;
    do_load(open_tables.at("USERTABLE"), "USERTABLE", hot_to_insert, hot_start_key, cold_to_insert, cold_start_key);
  }
  else {
    ALWAYS_ASSERT(ermia::config::numa_spread);
    ALWAYS_ASSERT(loader_id % ermia::config::numa_nodes == (uint32_t)me->node);
    LOG(INFO) << "Loading user table, " << ermia::config::numa_nodes * FLAGS_ycsb_hot_table_size << " hot records, "
      << ermia::config::numa_nodes * FLAGS_ycsb_cold_table_size << " cold records.";
    std::string table_name = std::string("USERTABLE") + std::to_string((int)me->node);
    uint32_t numa_local_loader_id = loader_id / ermia::config::numa_nodes;
    uint64_t hot_to_insert = ermia::config::numa_nodes * FLAGS_ycsb_hot_table_size / nloaders;
    uint64_t cold_to_insert = ermia::config::numa_nodes * FLAGS_ycsb_cold_table_size / nloaders;
    uint64_t hot_start_key = numa_local_loader_id * hot_to_insert;
    uint64_t cold_start_key = numa_local_loader_id * cold_to_insert;
    do_load(open_tables.at(table_name), table_name, hot_to_insert, hot_start_key, cold_to_insert, cold_start_key);
  }
}

void ycsb_table_loader::do_load(ermia::OrderedIndex *tbl, std::string table_name, 
    uint64_t hot_to_insert, uint64_t hot_start_key, uint64_t cold_to_insert, uint64_t cold_start_key) {
  uint32_t nloaders = std::thread::hardware_concurrency();
  int64_t to_insert = hot_to_insert + cold_to_insert;

  uint64_t kBatchSize = 256;

  // Load hot records.
  ermia::transaction *txn;
  for (uint64_t i_begin = 0; i_begin < hot_to_insert; i_begin += kBatchSize) {
    txn = db->NewTransaction(0, *arena, txn_buf());
    const uint64_t num = std::min(kBatchSize, hot_to_insert - i_begin);
#if defined(OLTPIM)
    oltpim::request_insert reqs[kBatchSize];
    auto *const main_index = (ermia::ConcurrentMasstreeIndex*)tbl;
#endif
    for (uint64_t j = 0; j < num; ++j) {
      const uint64_t i = i_begin + j;
      ermia::varstr &k = str(sizeof(ycsb_kv::key));
      BuildKey(hot_start_key + i, k);

      ermia::varstr &v = str(sizeof(ycsb_kv::value));
      new (&v) ermia::varstr((char *)&v + sizeof(ermia::varstr), sizeof(ycsb_kv::value));
      *(char *)v.p = 'a';

#if defined(OLTPIM)
      main_index->pim_InsertRecordBegin(txn, hot_start_key + i, v, &reqs[j]);
    }
    for (uint64_t j = 0; j < num; ++j) {
      TryVerifyStrict(sync_wait_oltpim_coro(main_index->pim_InsertRecordEnd(txn, &reqs[j])));
#else
#if defined(NESTED_COROUTINE)
      TryVerifyStrict(sync_wait_coro(tbl->InsertRecord(txn, k, v)));
#else
      TryVerifyStrict(tbl->InsertRecord(txn, k, v));
#endif
#endif // !defined(OLTPIM)

    }
    TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
  }

#if !defined(OLTPIM)
  // Load cold records.
  if (cold_to_insert) {
    txn = db->NewTransaction(0, *arena, txn_buf());
  }
  for (uint64_t i = 0; i < cold_to_insert; ++i) {
    ermia::varstr &k = str(sizeof(ycsb_kv::key));
    BuildKey(cold_start_key + i, k);

    ermia::varstr &v = str(sizeof(ycsb_kv::value));
    new (&v) ermia::varstr((char *)&v + sizeof(ermia::varstr), sizeof(ycsb_kv::value));
    *(char *)v.p = 'a';

#if defined(NESTED_COROUTINE)
      TryVerifyStrict(sync_wait_coro(tbl->InsertColdRecord(txn, k, v)));
#else
      TryVerifyStrict(tbl->InsertColdRecord(txn, k, v));
#endif

    if ((i + 1) % kBatchSize == 0 || i == cold_to_insert - 1) {
      TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
      if (i != cold_to_insert - 1) {
        txn = db->NewTransaction(0, *arena, txn_buf());
      }
    }
  }
#endif

  uint32_t n = ++loaders_barrier;
  while (loaders_barrier != nloaders && !all_flushed);

  if (n == nloaders) {
    ermia::dlog::flush_all();
    loaders_barrier = 0;
    all_flushed = true;
  }
  while (loaders_barrier);

#ifndef NDEBUG
  // Verify inserted values
  for (uint64_t i_begin = 0; i_begin < hot_to_insert; i_begin += kBatchSize) {
    txn = db->NewTransaction(0, *arena, txn_buf());
    const uint64_t num = std::min(kBatchSize, hot_to_insert - i_begin);
#if defined(OLTPIM)
    oltpim::request_get reqs[kBatchSize];
    auto *const main_index = (ermia::ConcurrentMasstreeIndex*)tbl;
#endif
    for (uint64_t j = 0; j < num; ++j) {
      const uint64_t i = i_begin + j;
      ermia::varstr &k = str(sizeof(ycsb_kv::key));
      BuildKey(hot_start_key + i, k);

#if defined(OLTPIM)
      uint64_t pk = hot_start_key + i;
      main_index->pim_GetRecordBegin(txn, pk, &reqs[j]);
    }
    for (uint64_t j = 0; j < num; ++j) {
      ermia::varstr &v = str(0);
      TryVerifyStrict(sync_wait_oltpim_coro(main_index->pim_GetRecordEnd(txn, v, &reqs[j])));
#else
      rc_t rc = rc_t{RC_INVALID};
      ermia::varstr &v = str(0);
#if defined(NESTED_COROUTINE)
      sync_wait_coro(tbl->GetRecord(txn, rc, k, v));
#else
      tbl->GetRecord(txn, rc, k, v);
#endif
      TryVerifyStrict(rc);
#endif // !defined(OLTPIM)
      ALWAYS_ASSERT(*(char *)v.data() == 'a');
    }
    TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
  }

#if !defined(OLTPIM)
  if (cold_to_insert) {
    txn = db->NewTransaction(0, *arena, txn_buf());
  }
  for (uint64_t i = 0; i < cold_to_insert; ++i) {
    rc_t rc = rc_t{RC_INVALID};
    ermia::varstr &k = str(sizeof(ycsb_kv::key));
    BuildKey(cold_start_key + i, k);
    ermia::varstr &v = str(0);
#if defined(NESTED_COROUTINE)
    sync_wait_coro(tbl->GetRecord(txn, rc, k, v));
#else
    tbl->GetRecord(txn, rc, k, v);
#endif
    ALWAYS_ASSERT(*(char *)v.data() == 'a');
    TryVerifyStrict(rc);

    if ((i + 1) % kBatchSize == 0 || i == cold_to_insert - 1) {
      TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
      if (i != cold_to_insert - 1) {
        txn = db->NewTransaction(0, *arena, txn_buf());
      }
    }
  }
#endif
#endif

  if (ermia::config::verbose) {
    std::cerr << "[INFO] loader " << loader_id << " loaded " << to_insert
              << " keys in " << table_name << std::endl;
  }
}

void ycsb_parse_options() {
  ermia::config::read_txn_type = FLAGS_ycsb_read_tx_type;
  if (FLAGS_ycsb_read_tx_type == "sequential") {
    g_read_txn_type = ReadTransactionType::Sequential;
  } else if (FLAGS_ycsb_read_tx_type == "nested-coro") {
    g_read_txn_type = ReadTransactionType::NestedCoro;
  } else if (FLAGS_ycsb_read_tx_type == "simple-coro") {
    g_read_txn_type = ReadTransactionType::SimpleCoro;
    ermia::config::coro_tx = true;
  } else if (FLAGS_ycsb_read_tx_type == "hybrid-coro") {
    g_read_txn_type = ReadTransactionType::HybridCoro;
  } else if (FLAGS_ycsb_read_tx_type == "flat-coro") {
    g_read_txn_type = ReadTransactionType::FlatCoro;
  } else if (FLAGS_ycsb_read_tx_type == "multiget-simple-coro") {
    g_read_txn_type = ReadTransactionType::SimpleCoroMultiGet;
    ermia::config::coro_tx = true;
  } else if (FLAGS_ycsb_read_tx_type == "multiget-nested-coro") {
    g_read_txn_type = ReadTransactionType::NestedCoroMultiGet;
  } else if (FLAGS_ycsb_read_tx_type == "multiget-amac") {
    g_read_txn_type = ReadTransactionType::AMACMultiGet;
  } else {
    LOG(FATAL) << "Wrong read transaction type " << std::string(optarg);
  }

  if (FLAGS_ycsb_workload == "A") {
    ycsb_workload = YcsbWorkloadA;
  } else if (FLAGS_ycsb_workload == "B") {
    ycsb_workload = YcsbWorkloadB;
  } else if (FLAGS_ycsb_workload == "C") {
    ycsb_workload = YcsbWorkloadC;
    ermia::config::coro_cold_tx_name = "1-ColdRead";
  } else if (FLAGS_ycsb_workload == "D") {
    ycsb_workload = YcsbWorkloadD;
  } else if (FLAGS_ycsb_workload == "E") {
    ycsb_workload = YcsbWorkloadE;
  } else if (FLAGS_ycsb_workload == "F") {
    ycsb_workload = YcsbWorkloadF;
    ermia::config::coro_cold_tx_name = "1-ColdRMW";
  } else if (FLAGS_ycsb_workload == "G") {
    ycsb_workload = YcsbWorkloadG;
  } else if (FLAGS_ycsb_workload == "I1") {
    ycsb_workload = YcsbWorkloadI1;
  } else if (FLAGS_ycsb_workload == "I2") {
    ycsb_workload = YcsbWorkloadI2;
  } else if (FLAGS_ycsb_workload == "I3") {
    ycsb_workload = YcsbWorkloadI3;
  } else if (FLAGS_ycsb_workload == "I4") {
    ycsb_workload = YcsbWorkloadI4;
  } else if (FLAGS_ycsb_workload == "S") {
    ycsb_workload = YcsbWorkloadS;
  } else {
    std::cerr << "Wrong workload type: " << FLAGS_ycsb_workload << std::endl;
    abort();
  }

  g_hot_table_zipfian = FLAGS_ycsb_hot_table_rng == "zipfian";

  ALWAYS_ASSERT(FLAGS_ycsb_hot_table_size);

  std::cerr << "ycsb settings:" << std::endl
            << "  workload:                   " << FLAGS_ycsb_workload << std::endl
            << "  hot table size:             " << FLAGS_ycsb_hot_table_size << std::endl
            << "  cold table size:            " << FLAGS_ycsb_cold_table_size << std::endl
            << "  operations per transaction: " << FLAGS_ycsb_ops_per_tx << std::endl
            << "  operations per hot transaction: " << FLAGS_ycsb_ops_per_hot_tx << std::endl
            << "  inserts per transaction: "    << FLAGS_ycsb_ins_per_tx << std::endl
            << "  updates per transaction: "    << FLAGS_ycsb_update_per_tx << std::endl
            << "  cold record reads per transaction: " << FLAGS_ycsb_cold_ops_per_tx << std::endl
            << "  additional reads after RMW: " << FLAGS_ycsb_rmw_additional_reads << std::endl
            << "  distribution:               " << FLAGS_ycsb_hot_table_rng << std::endl
            << "  read transaction type:      " << FLAGS_ycsb_read_tx_type << std::endl
            << "  simulated latency:          " << FLAGS_ycsb_latency << " microseconds" << std::endl;

  if (FLAGS_ycsb_hot_tx_percent) {
    std::cerr << "  hot read transaction percentage: " << FLAGS_ycsb_hot_tx_percent << std::endl;
  }
  if (FLAGS_ycsb_remote_tx_percent) {
    std::cerr << "  remote read transaction percentage: " << FLAGS_ycsb_remote_tx_percent << std::endl;
  }
  if (FLAGS_ycsb_hot_table_rng == "zipfian") {
    std::cerr << "  hot table zipfian theta:   " << FLAGS_ycsb_zipfian_theta << std::endl;
  }
  if (ycsb_workload.scan_percent() > 0) {
    if (FLAGS_ycsb_max_scan_size < g_scan_min_length || g_scan_min_length < 1) {
      std::cerr << "  invalid scan range:      " << std::endl;
      std::cerr << "  min :                    " << g_scan_min_length
                << std::endl;
      std::cerr << "  max :                    " << FLAGS_ycsb_max_scan_size
                << std::endl;
    }
    std::cerr << "  scan maximal range:         " << FLAGS_ycsb_max_scan_size
              << std::endl;
  }
}
