#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>

#include <stdlib.h>
#include <unistd.h>
#include <sys/sysinfo.h>
#include <sys/times.h>
#include <sys/wait.h>

#include "bench.h"

#include "../dbcore/rcu.h"
#include "../dbcore/sm-config.h"
#include "../dbcore/sm-table.h"

volatile bool running = true;
volatile uint64_t committed_txn_count = 0;

std::vector<bench_worker *> bench_runner::workers;

thread_local ermia::epoch_num coroutine_batch_end_epoch = 0;

void bench_worker::do_workload_function(uint32_t i) {
  ASSERT(workload.size());
retry:
  util::timer t;
  const unsigned long old_seed = r.get_seed();
  const auto ret = workload[i].fn(this);
  if (finish_workload(ret, i, t)) {
    r.set_seed(old_seed);
    goto retry;
  }
}

uint32_t bench_worker::fetch_workload() {
  if (fetch_cold_tx_interval) {
    if (fetch_cold_tx_interval == ermia::config::fetch_cold_tx_interval) {
      fetch_cold_tx_interval--;
      return 1;
    } else {
      if (fetch_cold_tx_interval == 1) {
        fetch_cold_tx_interval = ermia::config::fetch_cold_tx_interval;
      } else {
        fetch_cold_tx_interval--;
      }
      return 0;
    }
  } else {
    double d = r.next_uniform();
    for (size_t i = 0; i < workload.size(); i++) {
      if ((i + 1) == workload.size() || d < workload[i].frequency) {
          return i;
      }
      d -= workload[i].frequency;
    }
  }

  // unreachable
  return 0;
}

bool bench_worker::finish_workload(rc_t ret, uint32_t workload_idx, util::timer t) {
  if (!ret.IsAbort()) {
    if (ermia::config::benchmark_transactions &&
        __atomic_fetch_add(&committed_txn_count, 1, __ATOMIC_ACQ_REL) >= ermia::config::benchmark_transactions) {
      running = false;
      return false;
    }

    ++ntxn_commits;
    std::get<0>(txn_counts[workload_idx])++;
    if (!ermia::config::pcommit) {
      uint64_t lat = t.lap();
      latency_numer_us += lat;
      latency_hist_us.add(lat);
    }
    backoff_shifts >>= 1;
  } else {
    ++ntxn_aborts;
    std::get<1>(txn_counts[workload_idx])++;
    if (ret._val == RC_ABORT_USER) {
      std::get<3>(txn_counts[workload_idx])++;
    } else {
      std::get<2>(txn_counts[workload_idx])++;
    }
    switch (ret._val) {
      case RC_ABORT_SERIAL:
        inc_ntxn_serial_aborts();
        break;
      case RC_ABORT_SI_CONFLICT:
        inc_ntxn_si_aborts();
        break;
      case RC_ABORT_RW_CONFLICT:
        inc_ntxn_rw_aborts();
        break;
      case RC_ABORT_INTERNAL:
        inc_ntxn_int_aborts();
        break;
      case RC_ABORT_PHANTOM:
        inc_ntxn_phantom_aborts();
        break;
      case RC_ABORT_USER:
        inc_ntxn_user_aborts();
        break;
      default:
        ALWAYS_ASSERT(false);
    }
    if (ermia::config::retry_aborted_transactions && !ret.IsUserAbort() && running) {
      if (ermia::config::backoff_aborted_transactions) {
        if (backoff_shifts < 63) backoff_shifts++;
        uint64_t spins = 1UL << backoff_shifts;
        spins *= 100;  // XXX: tuned pretty arbitrarily
        while (spins) {
          NOP_PAUSE;
          spins--;
        }
      }
      return true;
    }
  }
  return false;
}

void bench_worker::MyWork(char *) {
  if (is_worker) {
    tlog = ermia::GetLog();
    workload = get_workload();
    txn_counts.resize(workload.size());
    barrier_a->count_down();
    barrier_b->wait_for();

    while (running) {
      uint32_t workload_idx = fetch_workload();
      do_workload_function(workload_idx);
    }
  }
}

void bench_runner::run() {
  // Get a thread to use benchmark-provided prepare(), which gathers
  // information about index pointers created by create_file_task.
  ermia::thread::Thread::Task runner_task =
    std::bind(&bench_runner::prepare, this, std::placeholders::_1);
  ermia::thread::Thread *runner_thread = ermia::thread::GetThread(ermia::thread::CoreType::PHYSICAL);
  runner_thread->StartTask(runner_task);
  runner_thread->Join();
  ermia::thread::PutThread(runner_thread);

  // load data, unless we recover from logs or is a backup server (recover from
  // shipped logs)
  ermia::volatile_write(ermia::config::state, ermia::config::kStateLoading);
  std::vector<bench_loader *> loaders = make_loaders();
  {
    util::scoped_timer t("dataloading", ermia::config::verbose);
    uint32_t done = 0;
    uint32_t n_running = 0;
  process:
    // if numa_spread, also spread loader threads across numa nodes.
    uint32_t n_loader_threads = std::thread::hardware_concurrency();
    ALWAYS_ASSERT(!ermia::config::numa_spread ||
      n_loader_threads % ermia::config::numa_nodes == 0);

    for (uint i = 0; i < loaders.size(); i++) {
      auto *loader = loaders[i];
      // Note: the thread pool creates threads for each hyperthread regardless
      // of how many worker threads will be running the benchmark. We don't
      // want to use threads on sockets that we won't be running benchmark on
      // for loading (that would cause some records' memory to become remote).
      // E.g., on a 40-core, 4 socket machine the thread pool will create 80
      // threads waiting to be dispatched. But if our workload only wants to
      // run 10 threads on the first socket, we won't want the loader to be run
      // on a thread from socket 2. So limit the number of concurrently running
      // loaders to the number of workers.
      if (loader && !loader->IsImpersonated() &&
          n_running < n_loader_threads &&
          (ermia::config::numa_spread ?
            loader->TryImpersonate(i % ermia::config::numa_nodes) :
            loader->TryImpersonate())) {
        loader->Start();
        ++n_running;
      }
    }

    // Loop over existing loaders to scavenge and reuse available threads
    while (done < loaders.size()) {
      for (uint i = 0; i < loaders.size(); i++) {
        auto *loader = loaders[i];
        if (loader and loader->IsImpersonated() and loader->TryJoin()) {
          delete loader;
          loaders[i] = nullptr;
          done++;
          --n_running;
          goto process;
        }
      }
    }


  }
  // FIXME:SSI safesnap to work with CSN.
  ermia::volatile_write(ermia::MM::safesnap_lsn, ermia::dlog::current_csn);
  ALWAYS_ASSERT(ermia::MM::safesnap_lsn);

  // Persist the database
  ermia::dlog::flush_all();
  if (ermia::config::pcommit) {
    ermia::dlog::dequeue_committed_xcts();
    // Sanity check to make sure all transactions are fully committed
    if (ermia::dlog::log_enabled()) {
      for (auto &tlog : ermia::dlog::tlogs) {
        LOG_IF(FATAL, tlog->get_commit_queue_size() > 0);
      }
    }
    // Enable latency recording
    ermia::dlog::set_record_latency(true);
  }
  ermia::dlog::signal_measure_start();

  // if (ermia::config::enable_chkpt) {
  //  ermia::chkptmgr->do_chkpt();  // this is synchronous
  // }

/*
  // Start checkpointer after database is ready
  if (ermia::config::enable_chkpt) {
    ermia::chkptmgr->start_chkpt_thread();
  }
  */
  ermia::volatile_write(ermia::config::state, ermia::config::kStateForwardProcessing);

  if (ermia::config::worker_threads) {
    start_measurement();
  }
}

void bench_runner::start_measurement() {
  workers = make_workers();
  ALWAYS_ASSERT(!workers.empty());
  for (std::vector<bench_worker *>::const_iterator it = workers.begin();
       it != workers.end(); ++it) {
    while (!(*it)->IsImpersonated()) {
      (*it)->TryImpersonate();
    }
    (*it)->Start();
  }

  pid_t perf_pid;
  if (ermia::config::enable_perf) {
    std::cerr << "start perf..." << std::endl;

    std::stringstream parent_pid;
    parent_pid << getpid();

    pid_t pid = fork();
    // Launch profiler
    if (pid == 0) {
      if(ermia::config::perf_record_event != "") {
        exit(execl("/usr/bin/perf","perf","record", "-F", "99", "-e", ermia::config::perf_record_event.c_str(), "--call-graph", "lbr",
                   "-p", parent_pid.str().c_str(), nullptr));
      } else {
        exit(execl("/usr/bin/perf","perf","stat", "-e", "LLC-load-misses,LLC-store-misses",
                   "-p", parent_pid.str().c_str(), nullptr));
      }
    } else {
      perf_pid = pid;
    }
  }
  pid_t eng_perf_pids[2];
  int eng_perf_fds[2][2];
  if (ermia::config::measure_energy) {
    // Measure CPU utilization, DRAM bandwidth, and UPMEM near-mem bandwidth
    // per-core CPU utilization
    if (pipe(eng_perf_fds[0]) != 0) {perror("pipe"); exit(1);}
    eng_perf_pids[0] = fork();
    if (eng_perf_pids[0] == 0) {
      close(eng_perf_fds[0][0]);
      dup2(eng_perf_fds[0][1], STDERR_FILENO);
      exit(execl("/usr/bin/perf", "perf", "stat", "-a", "--per-core", "-e",
        "msr/tsc/,cpu_clk_unhalted.ref_tsc,cpu_clk_unhalted.thread",
        nullptr));
    }
    close(eng_perf_fds[0][1]);
    // per-channel DRAM bandwidth
    if (pipe(eng_perf_fds[1]) != 0) {perror("pipe"); exit(1);}
    eng_perf_pids[1] = fork();
    if (eng_perf_pids[1] == 0) {
      close(eng_perf_fds[1][0]);
      dup2(eng_perf_fds[1][1], STDERR_FILENO);
      exit(execl("/usr/bin/perf", "perf", "stat", "-a", "-e",
        ermia::config::measure_energy_on_upmem_server ? (
          // Hardcoded for upmemcloud9
          "uncore_imc_0/cas_count_read/,uncore_imc_0/cas_count_write/,"
          "uncore_imc_1/cas_count_read/,uncore_imc_1/cas_count_write/,"
          "uncore_imc_2/cas_count_read/,uncore_imc_2/cas_count_write/,"
          "uncore_imc_3/cas_count_read/,uncore_imc_3/cas_count_write/,"
          "uncore_imc_4/cas_count_read/,uncore_imc_4/cas_count_write/,"
          "uncore_imc_5/cas_count_read/,uncore_imc_5/cas_count_write/"
        ) : (
          "uncore_imc/cas_count_read/,uncore_imc/cas_count_write/"
        ),
        nullptr));
    }
    close(eng_perf_fds[1][1]);
  }

  barrier_a.wait_for();  // wait for all threads to start up
  std::map<std::string, size_t> table_sizes_before;
  if (ermia::config::verbose) {
    //for (std::map<std::string, ermia::OrderedIndex *>::iterator it = open_tables.begin();
    //     it != open_tables.end(); ++it) {
    //  const size_t s = it->second->Size();
    //  std::cerr << "table " << it->first << " size " << s << std::endl;
    //  table_sizes_before[it->first] = s;
    //}
    std::cerr << "starting benchmark..." << std::endl;
  }

  // Print some results every second
  uint64_t slept = 0;
  uint64_t last_us = util::timer::cur_usec();
  uint64_t last_commits = 0, last_aborts = 0;
  uint64_t last_latency_numer_us = 0;

  // Print CPU utilization as well. Code adapted from:
  // https://stackoverflow.com/questions/63166/how-to-determine-cpu-and-memory-consumption-from-inside-a-process
  FILE* file;
  struct tms timeSample;
  char line[128];

  clock_t lastCPU = times(&timeSample);
  clock_t lastSysCPU = timeSample.tms_stime;
  clock_t lastUserCPU = timeSample.tms_utime;
  uint32_t nprocs = std::thread::hardware_concurrency();

  file = fopen("/proc/cpuinfo", "r");
  fclose(file);

  auto get_cpu_util = [&]() {
    ASSERT(ermia::config::print_cpu_util);
    struct tms timeSample;
    clock_t now;
    double percent;

    now = times(&timeSample);
    if (now <= lastCPU || timeSample.tms_stime < lastSysCPU ||
      timeSample.tms_utime < lastUserCPU){
      percent = -1.0;
    }
    else{
      percent = (timeSample.tms_stime - lastSysCPU) +
      (timeSample.tms_utime - lastUserCPU);
      percent /= (now - lastCPU);
      percent /= nprocs;
      percent *= 100;
    }
    lastCPU = now;
    lastSysCPU = timeSample.tms_stime;
    lastUserCPU = timeSample.tms_utime;
    return percent;
  };

#if defined(OLTPIM)
  oltpim::engine::g_engine.start_measurement();
#endif
  util::timer t, t_nosync;
  barrier_b.count_down();  // bombs away!

  double total_util = 0;
  double sec_util = 0;
  volatile uint64_t total_commits = 0;
  if (!ermia::config::benchmark_transactions) {
    // Time-based benchmark.
    if (ermia::config::print_cpu_util) {
      printf("Sec,Commits,Aborts,Tput,avgLat(us),CPU\n");
    } else {
      printf("Sec,Commits,Aborts,Tput,avgLat(us)\n");
    }

    auto gather_stats = [&]() {
      sleep(1);
      uint64_t sec_us = util::timer::cur_usec() - last_us;
      last_us += sec_us;
      uint64_t sec_commits = 0, sec_aborts = 0, sec_latency_numer_us = 0;
      for (size_t i = 0; i < ermia::config::worker_threads; i++) {
        sec_commits += workers[i]->get_ntxn_commits();
        sec_aborts += workers[i]->get_ntxn_aborts();
        sec_latency_numer_us += ermia::config::pcommit ?
          workers[i]->get_log()->get_latency() :
          workers[i]->get_latency_numer_us();
      }
      sec_commits -= last_commits;
      last_commits += sec_commits;
      sec_aborts -= last_aborts;
      last_aborts += sec_aborts;
      sec_latency_numer_us -= last_latency_numer_us;
      last_latency_numer_us += sec_latency_numer_us;

      double tput = (double)(sec_commits + sec_aborts) / ((double)sec_us / 1000000.0);
      double avg_latency_us = (double)(sec_latency_numer_us) / (sec_commits);

      if (ermia::config::print_cpu_util) {
        sec_util = get_cpu_util();
        total_util += sec_util;
        printf("%lu,%lu,%lu,%lf,%lf,%.2f%%\n",
          slept + 1, sec_commits, sec_aborts, tput, avg_latency_us, sec_util);
      } else {
        printf("%lu,%lu,%lu,%lf,%lf\n", 
          slept + 1, sec_commits, sec_aborts, tput, avg_latency_us);
      }
      slept++;
    };

    // Backups run forever until told to stop.
    while (slept < ermia::config::benchmark_seconds) {
      gather_stats();
    }

    running = false;
  } else {
    // Operation-based benchmark.
#if 0
    auto gather_stats = [&]() {
      sleep(1);
      total_commits = 0;
      for (size_t i = 0; i < ermia::config::worker_threads; i++) {
        total_commits += workers[i]->get_ntxn_commits();
      }
      printf("%lu,%.2f%%\n", slept + 1, 100 * double(total_commits) / ermia::config::benchmark_transactions);
      slept++;
    };
#endif

#if 0
    while (total_commits < ermia::config::benchmark_transactions) {
      //gather_stats();
      total_commits = 0;
      for (size_t i = 0; i < ermia::config::worker_threads; i++) {
        total_commits += workers[i]->get_ntxn_commits();
      }
    }

    running = false;
#endif
    while (ermia::volatile_read(running)) {}
  }

  ermia::volatile_write(ermia::config::state, ermia::config::kStateShutdown);
  for (size_t i = 0; i < ermia::config::worker_threads; i++) {
    workers[i]->Join();
  }

  const unsigned long elapsed_nosync = t_nosync.lap();

  if (ermia::config::measure_energy) {
    kill(eng_perf_pids[0], SIGINT);
    kill(eng_perf_pids[1], SIGINT);
    waitpid(eng_perf_pids[0], nullptr, 0);
    waitpid(eng_perf_pids[1], nullptr, 0);
  }
  if (ermia::config::enable_perf) {
    std::cerr << "stop perf..." << std::endl;
    kill(perf_pid, SIGINT);
    waitpid(perf_pid, nullptr, 0);
  }

  size_t n_commits = 0;
  size_t n_aborts = 0;
  size_t n_user_aborts = 0;
  size_t n_int_aborts = 0;
  size_t n_si_aborts = 0;
  size_t n_serial_aborts = 0;
  size_t n_rw_aborts = 0;
  size_t n_phantom_aborts = 0;
  size_t n_query_commits = 0;
  uint64_t latency_numer_us = 0;
  ermia::histogram_counter latency_hist_us;
  for (size_t i = 0; i < ermia::config::worker_threads; i++) {
    n_commits += workers[i]->get_ntxn_commits();
    n_aborts += workers[i]->get_ntxn_aborts();
    n_int_aborts += workers[i]->get_ntxn_int_aborts();
    n_user_aborts += workers[i]->get_ntxn_user_aborts();
    n_si_aborts += workers[i]->get_ntxn_si_aborts();
    n_serial_aborts += workers[i]->get_ntxn_serial_aborts();
    n_rw_aborts += workers[i]->get_ntxn_rw_aborts();
    n_phantom_aborts += workers[i]->get_ntxn_phantom_aborts();
    n_query_commits += workers[i]->get_ntxn_query_commits();
    if (ermia::config::pcommit) {
      latency_numer_us += workers[i]->get_log()->get_latency();
      latency_hist_us += workers[i]->get_log()->get_latency_hist();
    } else {
      latency_numer_us += workers[i]->get_latency_numer_us();
      latency_hist_us += workers[i]->latency_hist_us;
    }
  }

  const unsigned long elapsed = t.lap();
  const double elapsed_nosync_sec = double(elapsed_nosync) / 1000000.0;
  const uint64_t n_txns = n_commits + n_aborts;
  const double agg_nosync_throughput = double(n_txns) / elapsed_nosync_sec;
  const double avg_nosync_per_core_throughput =
      agg_nosync_throughput / double(workers.size());

  const double elapsed_sec = double(elapsed) / 1000000.0;
  const double agg_throughput = double(n_txns) / elapsed_sec;
  const double avg_per_core_throughput =
      agg_throughput / double(workers.size());

  const double agg_abort_rate = double(n_aborts) / elapsed_sec;
  const double avg_per_core_abort_rate =
      agg_abort_rate / double(workers.size());

  const double agg_system_abort_rate =
      double(n_aborts - n_user_aborts) / elapsed_sec;
  const double agg_user_abort_rate = double(n_user_aborts) / elapsed_sec;
  const double agg_int_abort_rate = double(n_int_aborts) / elapsed_sec;
  const double agg_si_abort_rate = double(n_si_aborts) / elapsed_sec;
  const double agg_serial_abort_rate = double(n_serial_aborts) / elapsed_sec;
  const double agg_phantom_abort_rate = double(n_phantom_aborts) / elapsed_sec;
  const double agg_rw_abort_rate = double(n_rw_aborts) / elapsed_sec;

  const double avg_latency_us = double(latency_numer_us) / double(n_commits);
  const double avg_latency_ms = avg_latency_us / 1000.0;
  const double p99_latency_us = latency_hist_us.pN(99);
  const double p99_latency_ms = p99_latency_us / 1000.0;

  uint64_t agg_latency_us = 0;
  uint64_t agg_redo_batches = 0;
  uint64_t agg_redo_size = 0;

  const double agg_replay_latency_ms = agg_latency_us / 1000.0;

  tx_stat_map agg_txn_counts = workers[0]->get_txn_counts();
  for (size_t i = 1; i < workers.size(); i++) {
    auto &c = workers[i]->get_txn_counts();
    for (auto &t : c) {
      std::get<0>(agg_txn_counts[t.first]) += std::get<0>(t.second);
      std::get<1>(agg_txn_counts[t.first]) += std::get<1>(t.second);
      std::get<2>(agg_txn_counts[t.first]) += std::get<2>(t.second);
      std::get<3>(agg_txn_counts[t.first]) += std::get<3>(t.second);
    }
  }

  double cpu_util, cpu_turbo_util;
  double dram_rd_mibps, dram_wr_mibps;
  double pim_rd_mibps, pim_wr_mibps;
  double pim_util, pim_mram_ratio, pim_mram_avg_size;
  if (ermia::config::measure_energy) {
    FILE *outf;
    char *tok_buf;
    size_t tok_i;
    std::vector<std::string> tokens;
    // per-core CPU utilization
    {
      outf = fdopen(eng_perf_fds[0][0], "r");
      tokens.clear();
      while (fscanf(outf, "%ms", &tok_buf) != EOF) {
        tokens.emplace_back(tok_buf);
        free(tok_buf);
      }
      fclose(outf);
      struct cpu_util_stats {
        uint64_t tsc, unhalted_ref, unhalted_core;
        bool operator<(const cpu_util_stats &other) const {
          return unhalted_core < other.unhalted_core;
        }
      };
      std::vector<cpu_util_stats> core_stats;
      cpu_util_stats core_stat;
      tok_i = 0;
      while (true) {
        while (tok_i < tokens.size() && tokens[tok_i] != "msr/tsc/") ++tok_i;
        if (tok_i >= tokens.size()) break;
        core_stat.tsc = std::stoull(tokens[tok_i-1]); ++tok_i;
        while (tokens[tok_i] != "cpu_clk_unhalted.ref_tsc") ++tok_i;
        core_stat.unhalted_ref = std::stoull(tokens[tok_i-1]); ++tok_i;
        while (tokens[tok_i] != "cpu_clk_unhalted.thread") ++tok_i;
        core_stat.unhalted_core = std::stoull(tokens[tok_i-1]); ++tok_i;
        core_stats.push_back(core_stat);
      }
      // Find top-"worker_threads" cores
      std::sort(core_stats.begin(), core_stats.end());
      uint32_t num_busy_cores = ermia::config::physical_workers_only ?
        ermia::config::worker_threads : ermia::config::worker_threads / 2;
      cpu_util = 0, cpu_turbo_util = 0;
      for (uint32_t i = 0; i < num_busy_cores; ++i) {
        auto &cs = core_stats[i];
        cpu_util += (double)cs.unhalted_ref / cs.tsc;
        cpu_turbo_util += (double)cs.unhalted_core / cs.tsc;
      }
      cpu_util /= num_busy_cores;
      cpu_turbo_util /= num_busy_cores;
    }

    // per-channel DRAM bandwidth
    outf = fdopen(eng_perf_fds[1][0], "r");
    tokens.clear();
    while (fscanf(outf, "%ms", &tok_buf) != EOF) {
      tokens.emplace_back(tok_buf);
      free(tok_buf);
    }
    fclose(outf);
    dram_rd_mibps = 0, dram_wr_mibps = 0;
    pim_rd_mibps = 0, pim_wr_mibps = 0;
    bool check_unit_correct = true;
    if (ermia::config::measure_energy_on_upmem_server) {
      // Channel 0,3 are DRAM; 1,2,4,5 are PIM
      const bool is_chn_dram[6] = {true, false, false, true, false, false};
      for (int chn = 0; chn < 6; ++chn) {
        std::string rd_event = std::string("uncore_imc_") + std::to_string(chn) + std::string("/cas_count_read/");
        std::string wr_event = std::string("uncore_imc_") + std::to_string(chn) + std::string("/cas_count_write/");
        for (tok_i = 0; tok_i < tokens.size(); ++tok_i) {
          bool is_read;
          if ((is_read = (tokens[tok_i] == rd_event)) || tokens[tok_i] == wr_event) {
            const double value = std::stod(tokens[tok_i-2]);
            if (is_chn_dram[chn]) {
              if (is_read) dram_rd_mibps += value;
              else dram_wr_mibps += value;
            }
            else {
              if (is_read) pim_rd_mibps += value;
              else pim_wr_mibps += value;
            }
            if (tokens[tok_i-1] != "MiB") check_unit_correct = false;
          }
        }
      }
    }
    else {
      for (tok_i = 0; tok_i < tokens.size(); ++tok_i) {
        if (tokens[tok_i] == "uncore_imc/cas_count_read/") {
          dram_rd_mibps += std::stod(tokens[tok_i-2]);
          if (tokens[tok_i-1] != "MiB") check_unit_correct = false;
        }
        else if (tokens[tok_i] == "uncore_imc/cas_count_write/") {
          dram_wr_mibps += std::stod(tokens[tok_i-2]);
          if (tokens[tok_i-1] != "MiB") check_unit_correct = false;
        }
      }
    }
    dram_rd_mibps /= elapsed_sec;
    dram_wr_mibps /= elapsed_sec;
    pim_rd_mibps /= elapsed_sec;
    pim_wr_mibps /= elapsed_sec;
    if (!check_unit_correct) {
      fprintf(stderr, "WARNING: %s:%d perf memory bandwidth unit mismatch.\n", __FILE__, __LINE__);
    }

    // UPMEM near-memory bandwidth
#if defined(OLTPIM)
    pim_util = 0, pim_mram_ratio = 0, pim_mram_avg_size = 0;
    double wram_ratio = 0;
    oltpim::engine::g_engine.compute_dpu_stats(elapsed_sec,
      pim_util, wram_ratio, pim_mram_ratio, pim_mram_avg_size);
#endif
  }

  //if (ermia::config::enable_chkpt) {
  //  delete ermia::chkptmgr;
  //}

  if (ermia::config::verbose) {
    //std::cerr << "--- table statistics ---" << std::endl;
    //for (std::map<std::string, ermia::OrderedIndex *>::iterator it = open_tables.begin();
    //     it != open_tables.end(); ++it) {
    //  const size_t s = it->second->Size();
    //  const ssize_t delta = ssize_t(s) - ssize_t(table_sizes_before[it->first]);
    //  std::cerr << "table " << it->first << " size " << it->second->Size();
    //  if (delta < 0)
    //    std::cerr << " (" << delta << " records)" << std::endl;
    //  else
    //    std::cerr << " (+" << delta << " records)" << std::endl;
    //}
    std::cerr << "--- benchmark statistics ---" << std::endl;
    std::cerr << "runtime: " << elapsed_sec << " sec" << std::endl;
    std::cerr << "cpu_util: " << total_util / elapsed_sec << "%" << std::endl;
    std::cerr << "agg_nosync_throughput: " << agg_nosync_throughput << " ops/sec"
         << std::endl;
    std::cerr << "avg_nosync_per_core_throughput: " << avg_nosync_per_core_throughput
         << " ops/sec/core" << std::endl;
    std::cerr << "agg_throughput: " << agg_throughput << " ops/sec" << std::endl;
    std::cerr << "avg_per_core_throughput: " << avg_per_core_throughput
         << " ops/sec/core" << std::endl;
    std::cerr << "avg_latency: " << avg_latency_ms << " ms" << std::endl;
    std::cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec" << std::endl;
    std::cerr << "avg_per_core_abort_rate: " << avg_per_core_abort_rate
         << " aborts/sec/core" << std::endl;
  }

  // output for plotting script
  std::cout << "---------------------------------------\n";
  std::cout
       << agg_throughput << " txns/s, "
       << p99_latency_ms << " latency.p99(ms), "
       << elapsed_sec << " total_time(sec), ";
  if (ermia::config::measure_energy) {
    std::cout << cpu_util << " cpu-util, "
          << cpu_turbo_util << " cpu-turbo-util, "
          << dram_rd_mibps << " dram.rd(MiB/s), "
          << dram_wr_mibps << " dram.wr(MiB/s), ";
    std::cout << pim_rd_mibps << " pim.rd(MiB/s), "
          << pim_wr_mibps << " pim.wr(MiB/s), "
          << pim_util << " pim-util, "
          << pim_mram_ratio << " pim-mram-ratio, "
          << pim_mram_avg_size << " pim-mram-size(B), ";
  }
  std::cout << std::endl;
  std::cout << "---------------------------------------\n";
  std::cout << agg_abort_rate << " total_aborts/s, " << agg_system_abort_rate
       << " system_aborts/s, " << agg_user_abort_rate << " user_aborts/s, "
       << agg_int_abort_rate << " internal aborts/s, " << agg_si_abort_rate
       << " si_aborts/s, " << agg_serial_abort_rate << " serial_aborts/s, "
       << agg_rw_abort_rate << " rw_aborts/s, " << agg_phantom_abort_rate
       << " phantom aborts/s." << std::endl;
  std::cout << n_commits << " commits, " << n_query_commits << " query_commits, "
       << n_aborts << " total_aborts, " << n_aborts - n_user_aborts
       << " system_aborts, " << n_user_aborts << " user_aborts, "
       << n_int_aborts << " internal_aborts, " << n_si_aborts << " si_aborts, "
       << n_serial_aborts << " serial_aborts, " << n_rw_aborts << " rw_aborts, "
       << n_phantom_aborts << " phantom_aborts" << std::endl;

  std::cout << "---------------------------------------\n";
  for (auto &c : agg_txn_counts) {
    std::cout << c.first << "\t" << std::get<0>(c.second) / (double)elapsed_sec
         << " commits/s\t" << std::get<1>(c.second) / (double)elapsed_sec
         << " aborts/s\t" << std::get<2>(c.second) / (double)elapsed_sec
         << " system aborts/s\t" << std::get<3>(c.second) / (double)elapsed_sec
         << " user aborts/s\n";
  }
  if (ermia::config::test_spinlock) {
    std::cout << "---------------------------------------\n";
    std::cout << "critical section size: " << ermia::config::test_spinlock_cs << std::endl;
  }
  std::cout.flush();
}

template <typename K, typename V>
struct map_maxer {
  typedef std::map<K, V> map_type;
  void operator()(map_type &agg, const map_type &m) const {
    for (typename map_type::const_iterator it = m.begin(); it != m.end(); ++it)
      agg[it->first] = std::max(agg[it->first], it->second);
  }
};

const tx_stat_map bench_worker::get_txn_counts() const {
  tx_stat_map m;
  const workload_desc_vec workload = get_workload();
  for (size_t i = 0; i < txn_counts.size(); i++)
    m[workload[i].name] = txn_counts[i];
  return m;
}

void bench_worker::PipelineScheduler() {
#ifdef GROUP_SAME_TRX
  LOG(FATAL) << "Pipeline scheduler doesn't work with batching same-type transactions";
#endif
  LOG(INFO) << "Epoch management and latency recorder in Pipeline scheduler are not logically correct";

  CoroTxnHandle *handles = (CoroTxnHandle *)numa_alloc_onnode(
    sizeof(CoroTxnHandle) * ermia::config::coro_batch_size, numa_node_of_cpu(sched_getcpu()));
  memset(handles, 0, sizeof(CoroTxnHandle) * ermia::config::coro_batch_size);

  uint32_t *workload_idxs = (uint32_t *)numa_alloc_onnode(
    sizeof(uint32_t) * ermia::config::coro_batch_size, numa_node_of_cpu(sched_getcpu()));

  rc_t *rcs = (rc_t *)numa_alloc_onnode(
    sizeof(rc_t) * ermia::config::coro_batch_size, numa_node_of_cpu(sched_getcpu()));

  barrier_a->count_down();
  barrier_b->wait_for();
  util::timer t;

  for (uint32_t i = 0; i < ermia::config::coro_batch_size; i++) {
    uint32_t workload_idx = fetch_workload();
    workload_idxs[i] = workload_idx;
    handles[i] = workload[workload_idx].coro_fn(this, i, 0).get_handle();
  }

  uint32_t i = 0;
  coroutine_batch_end_epoch = 0;
  ermia::epoch_num begin_epoch = ermia::MM::epoch_enter();
  while (running) {
    if (handles[i].done()) {
      rcs[i] = handles[i].promise().get_return_value();
#ifdef CORO_BATCH_COMMIT
      if (!rcs[i].IsAbort()) {
        rcs[i] = db->Commit(&transactions[i]);
      }
#endif
      finish_workload(rcs[i], workload_idxs[i], t);
      handles[i].destroy();

      uint32_t workload_idx = fetch_workload();
      workload_idxs[i] = workload_idx;
      handles[i] = workload[workload_idx].coro_fn(this, i, 0).get_handle();
    } else if (!handles[i].promise().callee_coro || handles[i].promise().callee_coro.done()) {
      handles[i].resume();
    } else {
      handles[i].promise().callee_coro.resume();
    }

    i = (i + 1) & (ermia::config::coro_batch_size - 1);
  }

  ermia::MM::epoch_exit(coroutine_batch_end_epoch, begin_epoch);
}


void bench_worker::Scheduler() {
#ifdef GROUP_SAME_TRX
  LOG(FATAL) << "General scheduler doesn't work with batching same-type transactions";
#endif
#ifdef CORO_BATCH_COMMIT
  LOG(FATAL) << "General scheduler doesn't work with batching commits";
#endif
  CoroTxnHandle *handles = (CoroTxnHandle *)numa_alloc_onnode(
    sizeof(CoroTxnHandle) * ermia::config::coro_batch_size, numa_node_of_cpu(sched_getcpu()));
  memset(handles, 0, sizeof(CoroTxnHandle) * ermia::config::coro_batch_size);

  uint32_t *workload_idxs = (uint32_t *)numa_alloc_onnode(
    sizeof(uint32_t) * ermia::config::coro_batch_size, numa_node_of_cpu(sched_getcpu()));

  rc_t *rcs = (rc_t *)numa_alloc_onnode(
    sizeof(rc_t) * ermia::config::coro_batch_size, numa_node_of_cpu(sched_getcpu()));

  barrier_a->count_down();
  barrier_b->wait_for();

  while (running) {
    coroutine_batch_end_epoch = 0;
    ermia::epoch_num begin_epoch = ermia::MM::epoch_enter();
    uint32_t todo = ermia::config::coro_batch_size;
    util::timer t;

    for (uint32_t i = 0; i < ermia::config::coro_batch_size; i++) {
      uint32_t workload_idx = fetch_workload();
      workload_idxs[i] = workload_idx;
      handles[i] = workload[workload_idx].coro_fn(this, i, 0).get_handle();
    }

    while (todo) {
      for (uint32_t i = 0; i < ermia::config::coro_batch_size; i++) {
        if (!handles[i]) {
          continue;
        }
        if (handles[i].done()) {
          rcs[i] = handles[i].promise().get_return_value();
          finish_workload(rcs[i], workload_idxs[i], t);
          handles[i].destroy();
          handles[i] = nullptr;
          --todo;
        } else if (!handles[i].promise().callee_coro || handles[i].promise().callee_coro.done()) {
          handles[i].resume();
        } else {
          handles[i].promise().callee_coro.resume();
        }
      }
    }

    ermia::MM::epoch_exit(coroutine_batch_end_epoch, begin_epoch);
  }
}

void bench_worker::GroupScheduler() {
  CoroTxnHandle *handles = (CoroTxnHandle *)numa_alloc_onnode(
    sizeof(CoroTxnHandle) * ermia::config::coro_batch_size, numa_node_of_cpu(sched_getcpu()));
  memset(handles, 0, sizeof(CoroTxnHandle) * ermia::config::coro_batch_size);

  rc_t *rcs = (rc_t *)numa_alloc_onnode(
    sizeof(rc_t) * ermia::config::coro_batch_size, numa_node_of_cpu(sched_getcpu()));

#ifndef GROUP_SAME_TRX
  LOG(FATAL) << "Group scheduler batches same-type transactions";
#endif

  barrier_a->count_down();
  barrier_b->wait_for();

  while (running) {
    coroutine_batch_end_epoch = 0;
    ermia::epoch_num begin_epoch = ermia::MM::epoch_enter();
    uint32_t todo = ermia::config::coro_batch_size;
    uint32_t workload_idx = -1;
    workload_idx = fetch_workload();
    util::timer t;

    for (uint32_t i = 0; i < ermia::config::coro_batch_size; i++) {
      handles[i] = workload[workload_idx].coro_fn(this, i, 0).get_handle();
    }

    while (todo) {
      for (uint32_t i = 0; i < ermia::config::coro_batch_size; i++) {
        if (!handles[i]) {
          continue;
        }
        if (handles[i].done()) {
          rcs[i] = handles[i].promise().get_return_value();
#ifndef CORO_BATCH_COMMIT
          finish_workload(rcs[i], workload_idx, t);
#endif
          handles[i].destroy();
          handles[i] = nullptr;
          --todo;
        } else if (!handles[i].promise().callee_coro || handles[i].promise().callee_coro.done()) {
          handles[i].resume();
        } else {
          handles[i].promise().callee_coro.resume();
        }
      }
    }

#ifdef CORO_BATCH_COMMIT
    for (uint32_t i = 0; i < ermia::config::coro_batch_size; i++) {
      if (!rcs[i].IsAbort()) {
        rcs[i] = db->Commit(&transactions[i]);
      }
      // No need to abort - TryCatchCond family of macros should have already
      finish_workload(rcs[i], workload_idx, t);
    }
#endif

    ermia::MM::epoch_exit(coroutine_batch_end_epoch, begin_epoch);
  }
}


