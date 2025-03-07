#pragma once

#include <sys/time.h>
#include <string>
#include <ctype.h>
#include <unistd.h>
#include <algorithm>

#include "../bench.h"
#include "../dbtest.h"
#include "tpcc.h"
#if defined(OLTPIM)
#include "tpcc-key64.h"
#endif

// configuration flags
// XXX(shiges): For compatibility issues, we keep gflags configs and the old
// configs separately. Eventually we want to merge them together.
DECLARE_uint64(tpcc_scale_factor);
DECLARE_bool(tpcc_disable_xpartition_txn);
DECLARE_bool(tpcc_enable_separate_tree_per_partition);
DECLARE_uint64(tpcc_new_order_remote_item_pct);
DECLARE_bool(tpcc_new_order_fast_id_gen);
DECLARE_bool(tpcc_uniform_item_dist);
DECLARE_bool(tpcc_order_status_scan_hack);
DECLARE_uint64(tpcc_wh_temperature);
DECLARE_uint64(tpcc_microbench_rows);  // this many rows
// can't have both ratio and rows at the same time
DECLARE_uint64(tpcc_microbench_wr_rows);  // this number of rows to write
DECLARE_uint64(tpcc_nr_suppliers);
DECLARE_uint64(tpcc_hybrid);

DECLARE_string(tpcc_txn_workload_mix);
DECLARE_double(tpcc_wh_spread);

DECLARE_uint32(tpcc_cold_customer_pct);
DECLARE_uint32(tpcc_cold_item_pct);

DECLARE_bool(tpcc_coro_local_wh);
DECLARE_bool(tpcc_numa_local);
DECLARE_bool(tpcc_atomic_ytd);

extern int g_wh_temperature;
extern uint g_microbench_rows;  // this many rows
// can't have both ratio and rows at the same time
extern int g_microbench_wr_rows;  // this number of rows to write
extern int g_hybrid;

extern unsigned g_txn_workload_mix[8];

extern util::aligned_padded_elem<std::atomic<uint64_t>> *g_district_ids ;

typedef std::vector<std::vector<std::pair<int32_t, int32_t>>> SuppStockMap;
extern SuppStockMap supp_stock_map;

extern util::aligned_padded_elem<std::atomic<float>> *g_warehouse_ytds;
extern util::aligned_padded_elem<std::atomic<float>> *g_district_ytds;

// config constants
struct Nation {
  int id;
  std::string name;
  int rId;
};
extern const Nation nations[];

extern const char *regions[];

static constexpr ALWAYS_INLINE size_t NumItems() { return 100000; }

static constexpr ALWAYS_INLINE size_t NumDistrictsPerWarehouse() { return 10; }

static constexpr ALWAYS_INLINE size_t NumCustomersPerDistrict() { return 3000; }

static ALWAYS_INLINE size_t NumWarehouses() {
  return FLAGS_tpcc_scale_factor;
}

static inline std::atomic<uint64_t> &NewOrderIdHolder(unsigned warehouse,
                                                 unsigned district) {
  ASSERT(warehouse >= 1 && warehouse <= NumWarehouses());
  ASSERT(district >= 1 && district <= NumDistrictsPerWarehouse());
  const unsigned idx =
      (warehouse - 1) * NumDistrictsPerWarehouse() + (district - 1);
  return g_district_ids[idx].elem;
}

static inline uint64_t FastNewOrderIdGen(unsigned warehouse,
                                         unsigned district) {
  return NewOrderIdHolder(warehouse, district)
      .fetch_add(1, std::memory_order_acq_rel);
}

static inline void atomic_float_fetch_add(std::atomic<float> &v,
                                          float amount) {
  float oldv = v.load(std::memory_order_relaxed);
  float newv;
  do {newv = oldv + amount;}
  while (!v.compare_exchange_weak(oldv, newv,
    std::memory_order_release, std::memory_order_relaxed));
}

static inline void AtomicAddYtd(unsigned warehouse, unsigned district,
                                float amount) {
  ASSERT(warehouse >= 1 && warehouse <= NumWarehouses());
  ASSERT(district >= 1 && district <= NumDistrictsPerWarehouse());
  
  atomic_float_fetch_add(g_warehouse_ytds[warehouse - 1].elem, amount);
  const unsigned idx =
      (warehouse - 1) * NumDistrictsPerWarehouse() + (district - 1);
  atomic_float_fetch_add(g_district_ytds[idx].elem, amount);
}

struct eqstr {
  bool operator()(const char *s1, const char *s2) const {
    return (s1 == s2) || (s1 && s2 && strcmp(s1, s2) == 0);
  }
};

#ifndef NDEBUG
struct checker {
  // these sanity checks are just a few simple checks to make sure
  // the data is not entirely corrupted

  static ALWAYS_INLINE void SanityCheckCustomer(
      const customer::key *k, const customer::value *v) {
    ASSERT(v->c_credit == "BC" || v->c_credit == "GC");
    ASSERT(v->c_middle == "OE");
    ASSERT(k->c_w_id >= 1 && static_cast<size_t>(k->c_w_id) <= NumWarehouses());
    ASSERT(k->c_d_id >= 1 &&
           static_cast<size_t>(k->c_d_id) <= NumDistrictsPerWarehouse());
    ASSERT(k->c_id >= 1 &&
           static_cast<size_t>(k->c_id) <= NumCustomersPerDistrict());
  }

  static ALWAYS_INLINE void SanityCheckWarehouse(
      const warehouse::key *k, const warehouse::value *v) {
    ASSERT(k->w_id >= 1 && static_cast<size_t>(k->w_id) <= NumWarehouses());
    ASSERT(v->w_state.size() == 2);
    ASSERT(v->w_zip == "123456789");
  }

  static ALWAYS_INLINE void SanityCheckDistrict(
      const district::key *k, const district::value *v) {
    ASSERT(k->d_w_id >= 1 && static_cast<size_t>(k->d_w_id) <= NumWarehouses());
    ASSERT(k->d_id >= 1 &&
           static_cast<size_t>(k->d_id) <= NumDistrictsPerWarehouse());
    ASSERT(v->d_next_o_id >= 3001);
    ASSERT(v->d_state.size() == 2);
    ASSERT(v->d_zip == "123456789");
  }

  static ALWAYS_INLINE void SanityCheckItem(const item::key *k,
                                                   const item::value *v) {
    ASSERT(k->i_id >= 1 && static_cast<size_t>(k->i_id) <= NumItems());
    ASSERT(v->i_price >= 1.0 && v->i_price <= 100.0);
  }

  static ALWAYS_INLINE void SanityCheckStock(const stock::key *k) {
    ASSERT(k->s_w_id >= 1 && static_cast<size_t>(k->s_w_id) <= NumWarehouses());
    ASSERT(k->s_i_id >= 1 && static_cast<size_t>(k->s_i_id) <= NumItems());
  }

  static ALWAYS_INLINE void SanityCheckNewOrder(const new_order::key *k) {
    ASSERT(k->no_w_id >= 1 &&
           static_cast<size_t>(k->no_w_id) <= NumWarehouses());
    ASSERT(k->no_d_id >= 1 &&
           static_cast<size_t>(k->no_d_id) <= NumDistrictsPerWarehouse());
  }

  static ALWAYS_INLINE void SanityCheckOOrder(const oorder::key *k,
                                                     const oorder::value *v) {
    ASSERT(k->o_w_id >= 1 && static_cast<size_t>(k->o_w_id) <= NumWarehouses());
    ASSERT(k->o_d_id >= 1 &&
           static_cast<size_t>(k->o_d_id) <= NumDistrictsPerWarehouse());
    ASSERT(v->o_c_id >= 1 &&
           static_cast<size_t>(v->o_c_id) <= NumCustomersPerDistrict());
    ASSERT(v->o_carrier_id >= 0 &&
           static_cast<size_t>(v->o_carrier_id) <= NumDistrictsPerWarehouse());
    ASSERT(v->o_ol_cnt >= 5 && v->o_ol_cnt <= 15);
  }

  static ALWAYS_INLINE void SanityCheckOrderLine(
      const order_line::key *k, const order_line::value *v) {
    ASSERT(k->ol_w_id >= 1 &&
           static_cast<size_t>(k->ol_w_id) <= NumWarehouses());
    ASSERT(k->ol_d_id >= 1 &&
           static_cast<size_t>(k->ol_d_id) <= NumDistrictsPerWarehouse());
    ASSERT(k->ol_number >= 1 && k->ol_number <= 15);
    ASSERT(v->ol_i_id >= 1 && static_cast<size_t>(v->ol_i_id) <= NumItems());
  }
};
#endif

class tpcc_table_scanner : public ermia::OrderedIndex::ScanCallback {
 public:
  tpcc_table_scanner(ermia::str_arena *arena) : _arena(arena) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    ermia::varstr *const k = _arena->next(keylen);
    ermia::varstr *v = _arena->next(0);  // header only
    v->p = value.p;
    v->l = value.l;
    ASSERT(k);
    k->copy_from(keyp, keylen);
    output.emplace_back(k, v);
    return true;
  }

  void clear() { output.clear(); }
  std::vector<std::pair<ermia::varstr *, const ermia::varstr *>> output;
  ermia::str_arena *_arena;
};

class tpcc_worker_mixin : private _dummy {
#define DEFN_TBL_INIT_X(name) , tbl_##name##_vec(partitions.at(#name))

 public:
  tpcc_worker_mixin(const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions)
      : _dummy()  // so hacky...
        TPCC_TABLE_LIST(DEFN_TBL_INIT_X) {
    ALWAYS_ASSERT(NumWarehouses() >= 1);
  }

#undef DEFN_TBL_INIT_X

 protected:
#define DEFN_TBL_ACCESSOR_X(name)                                              \
 private:                                                                      \
  std::vector<ermia::OrderedIndex *> tbl_##name##_vec;                         \
                                                                               \
 protected:                                                                    \
  ALWAYS_INLINE ermia::ConcurrentMasstreeIndex *tbl_##name(unsigned int wid) { \
    ASSERT(wid >= 1 && wid <= NumWarehouses());                                \
    ASSERT(tbl_##name##_vec.size() == NumWarehouses());                        \
    return (ermia::ConcurrentMasstreeIndex *)tbl_##name##_vec[wid - 1];        \
  }

  TPCC_TABLE_LIST(DEFN_TBL_ACCESSOR_X)

#undef DEFN_TBL_ACCESSOR_X

 public:
  static inline uint32_t GetCurrentTimeMillis() {
    // struct timeval tv;
    // ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
    // return tv.tv_sec * 1000;

    // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
    // for now, we just give each core an increasing number

    static thread_local uint32_t tl_hack = 0;
    return tl_hack++;
  }

  // utils for generating random #s and strings

  static ALWAYS_INLINE int CheckBetweenInclusive(int v, int lower,
                                                        int upper) {
    MARK_REFERENCED(lower);
    MARK_REFERENCED(upper);
    ASSERT(v >= lower);
    ASSERT(v <= upper);
    return v;
  }

  static ALWAYS_INLINE int RandomNumber(util::fast_random &r, int min,
                                               int max) {
    return CheckBetweenInclusive(
        (int)(r.next_uniform() * (max - min + 1) + min), min, max);
  }

  static ALWAYS_INLINE int NonUniformRandom(util::fast_random &r, int A, int C,
                                                   int min, int max) {
    return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) %
            (max - min + 1)) +
           min;
  }

  static ALWAYS_INLINE int GetItemId(util::fast_random &r) {
    return CheckBetweenInclusive(
        FLAGS_tpcc_uniform_item_dist ? RandomNumber(r, 1, NumItems())
                            : NonUniformRandom(r, 8191, 7911, 1, NumItems()),
        1, NumItems());
  }

  static ALWAYS_INLINE int GetCustomerId(util::fast_random &r) {
    return CheckBetweenInclusive(
        NonUniformRandom(r, 1023, 259, 1, NumCustomersPerDistrict()), 1,
        NumCustomersPerDistrict());
  }

  static std::string NameTokens[];

  // all tokens are at most 5 chars long
  static const size_t CustomerLastNameMaxSize = 5 * 3;

  static inline size_t GetCustomerLastName(uint8_t *buf, int num) {
    const std::string &s0 = NameTokens[num / 100];
    const std::string &s1 = NameTokens[(num / 10) % 10];
    const std::string &s2 = NameTokens[num % 10];
    uint8_t *const begin = buf;
    const size_t s0_sz = s0.size();
    const size_t s1_sz = s1.size();
    const size_t s2_sz = s2.size();
    memcpy(buf, s0.data(), s0_sz);
    buf += s0_sz;
    memcpy(buf, s1.data(), s1_sz);
    buf += s1_sz;
    memcpy(buf, s2.data(), s2_sz);
    buf += s2_sz;
    return buf - begin;
  }

  static inline std::string GetCustomerLastName(int num) {
    std::string ret;
    ret.resize(CustomerLastNameMaxSize);
    ret.resize(GetCustomerLastName((uint8_t *)&ret[0], num));
    return ret;
  }

  static ALWAYS_INLINE std::string
  GetNonUniformCustomerLastNameLoad(util::fast_random &r) {
    return GetCustomerLastName(NonUniformRandom(r, 255, 157, 0, 999));
  }

  static ALWAYS_INLINE size_t
  GetNonUniformCustomerLastNameRun(uint8_t *buf, util::fast_random &r) {
    return GetCustomerLastName(buf, NonUniformRandom(r, 255, 223, 0, 999));
  }

  static ALWAYS_INLINE size_t
  GetNonUniformCustomerLastNameRun(char *buf, util::fast_random &r) {
    return GetNonUniformCustomerLastNameRun((uint8_t *)buf, r);
  }

  static ALWAYS_INLINE std::string
  GetNonUniformCustomerLastNameRun(util::fast_random &r) {
    return GetCustomerLastName(NonUniformRandom(r, 255, 223, 0, 999));
  }

  // following oltpbench, we really generate strings of len - 1...
  static inline std::string RandomStr(util::fast_random &r, uint len) {
    // this is a property of the oltpbench implementation...
    if (!len) return "";

    uint i = 0;
    std::string buf(len - 1, 0);
    while (i < (len - 1)) {
      const char c = (char)r.next_char();
      // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
      // is a less restrictive filter than isalnum()
      if (!isalnum(c)) continue;
      buf[i++] = c;
    }
    return buf;
  }

  // RandomNStr() actually produces a std::string of length len
  static inline std::string RandomNStr(util::fast_random &r, uint len) {
    const char base = '0';
    std::string buf(len, 0);
    for (uint i = 0; i < len; i++) buf[i] = (char)(base + (r.next() % 10));
    return buf;
  }

  // 80/20 access: 80% of all accesses touch 20% of WHs (randmonly
  // choose one from hot_whs), while the 20% of accesses touch the
  // remaining 80% of WHs.
  static std::vector<uint> hot_whs;
  static std::vector<uint> cold_whs;

  ALWAYS_INLINE unsigned pick_home_wh(util::fast_random &r, uint worker_id, uint batch_id, uint total_batches) {
    // worker_id is in [0, worker_threads)
    // batch_id is in [0, coro_batch_size + coro_cold_queue_size)
    // basically assign to global_id = worker_id + batch_id * worker_threads
    const uint global_id = worker_id + batch_id * ermia::config::worker_threads;
    // total_batches is pre-computed as worker_threads * (coro_batch_size + coro_cold_queue_size)
    if (total_batches >= NumWarehouses()) {
      return 1 + global_id % NumWarehouses();
    }
    else {
      return 1 + (r.next() % (NumWarehouses() / total_batches)) * total_batches + global_id;
    }
  }

  ALWAYS_INLINE unsigned pick_wh(util::fast_random &r, uint home_wh) {
    if (g_wh_temperature) {  // do it 80/20 way
      uint w = 0;
      if (r.next_uniform() >= 0.2)  // 80% access
        w = hot_whs[r.next() % hot_whs.size()];
      else
        w = cold_whs[r.next() % cold_whs.size()];
      LOG_IF(FATAL, w < 1 || w > NumWarehouses());
      return w;
    } else {
      ASSERT(FLAGS_tpcc_wh_spread >= 0 and FLAGS_tpcc_wh_spread <= 1);
      // wh_spread = 0: always use home wh
      // wh_spread = 1: always use random wh
      if (FLAGS_tpcc_wh_spread == 0 || r.next_uniform() >= FLAGS_tpcc_wh_spread)
        return home_wh;
      return r.next() % NumWarehouses() + 1;
    }
  }

};

class tpcc_nation_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_nation_loader(unsigned long seed, ermia::Engine *db,
                     const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                     const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    std::string obj_buf;
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    uint i;
    uint64_t total_sz = 0;
    for (i = 0; i < 62; i++) {
      const nation::key k(nations[i].id);
      nation::value v;
      total_sz += Size(v);

      const std::string n_comment = RandomStr(r, RandomNumber(r, 10, 20));
      v.n_name = std::string(nations[i].name);
      v.n_regionkey = nations[i].rId;
      v.n_comment.assign(n_comment);
#if defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
      TryVerifyStrict(sync_wait_coro(tbl_nation(1)->InsertRecord(txn, Encode(str(Size(k)), k),
                                              Encode(str(Size(v)), v))));
#else
      TryVerifyStrict(tbl_nation(1)->InsertRecord(txn, Encode(str(Size(k)), k),
                                              Encode(str(Size(v)), v)));
#endif
    }
    TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
    LOG(INFO) << "Finished loading nation";
    LOG(INFO) << "  * total/average nation record length: "
         << total_sz << "/" << (double(total_sz) / double(62)) << " bytes";
  }
};

class tpcc_region_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_region_loader(unsigned long seed, ermia::Engine *db,
                     const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                     const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    uint64_t total_sz = 0;
    std::string obj_buf;
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    for (uint i = 0; i < 5; i++) {
      const region::key k(i);
      region::value v;

      v.r_name = std::string(regions[i]);
      const std::string r_comment = RandomStr(r, RandomNumber(r, 10, 20));
      v.r_comment.assign(r_comment);
#if defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
      TryVerifyStrict(sync_wait_coro(tbl_region(1)->InsertRecord(txn, Encode(str(Size(k)), k),
                                              Encode(str(Size(v)), v))));
#else
      TryVerifyStrict(tbl_region(1)->InsertRecord(txn, Encode(str(Size(k)), k),
                                              Encode(str(Size(v)), v)));
#endif
      total_sz += Size(v);
    }
    TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
    LOG(INFO) << "Finished loading region";
    LOG(INFO) << "  * total/average region record length: "
         << total_sz << "/" << (double(total_sz) / double(5)) << " bytes";
  }
};

class tpcc_supplier_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_supplier_loader(unsigned long seed, ermia::Engine *db,
                       const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                       const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    uint64_t total_sz = 0;
    std::string obj_buf;
    for (uint i = 0; i < 10000; i++) {
      ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
      const supplier::key k(i);
      supplier::value v;

      v.su_name = std::string("Supplier#") + std::string("000000000") + std::to_string(i);
      v.su_address = RandomStr(r, RandomNumber(r, 10, 40));

      auto rand = 0;
      while (rand == 0 || (rand > '9' && rand < 'A') ||
             (rand > 'Z' && rand < 'a'))
        rand = RandomNumber(r, '0', 'z');
      v.su_nationkey = rand;
      //		  v.su_phone = std::string("911");			//
      //XXX. nobody wants this field
      //		  v.su_acctbal = 0;
      //		  v.su_comment = RandomStr(r, RandomNumber(r,10,39));
      //// XXX. Q16 uses this. fix this if needed.

#if defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
      TryVerifyStrict(sync_wait_coro(tbl_supplier(1)->InsertRecord(txn, Encode(str(Size(k)), k),
                                                Encode(str(Size(v)), v))));
#else
      TryVerifyStrict(tbl_supplier(1)->InsertRecord(txn, Encode(str(Size(k)), k),
                                                Encode(str(Size(v)), v)));
#endif

      TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
      total_sz += Size(v);
    }
    LOG(INFO) << "Finished loading supplier";
    LOG(INFO) << "  * total/average supplier record length: "
         << total_sz << "/" << (double(total_sz) / double(10000)) << " bytes";
  }
};

class tpcc_warehouse_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_warehouse_loader(unsigned long seed, ermia::Engine *db,
                        const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                        const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions,
                        int loader_id = 0)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions), loader_id(loader_id) {}

 protected:
  int loader_id;
  virtual void load() {
    if (FLAGS_tpcc_numa_local) ALWAYS_ASSERT(loader_id == me->node);
    std::string obj_buf;
    uint64_t warehouse_total_sz = 0, n_warehouses = 0;
    std::vector<warehouse::value> warehouses;
    for (uint i = 1; i <= NumWarehouses(); i++) {
      if (FLAGS_tpcc_numa_local && ((i-1)%ermia::config::numa_nodes != loader_id)) continue;
      arena->reset();
      ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
      const warehouse::key k(i);

      const std::string w_name = RandomStr(r, RandomNumber(r, 6, 10));
      const std::string w_street_1 = RandomStr(r, RandomNumber(r, 10, 20));
      const std::string w_street_2 = RandomStr(r, RandomNumber(r, 10, 20));
      const std::string w_city = RandomStr(r, RandomNumber(r, 10, 20));
      const std::string w_state = RandomStr(r, 3);
      const std::string w_zip = "123456789";

      warehouse::value v;
      v.w_ytd = 300000;
      v.w_tax = (float)RandomNumber(r, 0, 2000) / 10000.0;
      v.w_name.assign(w_name);
      v.w_street_1.assign(w_street_1);
      v.w_street_2.assign(w_street_2);
      v.w_city.assign(w_city);
      v.w_state.assign(w_state);
      v.w_zip.assign(w_zip);

#ifndef NDEBUG
      checker::SanityCheckWarehouse(&k, &v);
#endif
      const size_t sz = Size(v);
      warehouse_total_sz += sz;
      n_warehouses++;
#if defined(OLTPIM)
      TryVerifyStrict(sync_wait_oltpim_coro(tbl_warehouse(i)->pim_InsertRecord(txn, tpcc_key64::warehouse(k), Encode(str(sz), v))));
#elif defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
      TryVerifyStrict(sync_wait_coro(tbl_warehouse(i)->InsertRecord(txn, Encode(str(Size(k)), k),
                                                 Encode(str(sz), v))));
#else
      TryVerifyStrict(tbl_warehouse(i)->InsertRecord(txn, Encode(str(Size(k)), k),
                                                 Encode(str(sz), v)));
#endif

      warehouses.push_back(v);
      TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
    }
    for (uint i = 1; i <= NumWarehouses(); i++) {
      if (FLAGS_tpcc_numa_local && ((i-1)%ermia::config::numa_nodes != loader_id)) continue;
      arena->reset();
      ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
      const warehouse::key k(i);
      warehouse::value warehouse_temp;
      ermia::varstr warehouse_v;

      rc_t rc = rc_t{RC_INVALID};
#if defined(OLTPIM)
      uint64_t pk = tpcc_key64::warehouse(k);
      rc = sync_wait_oltpim_coro(tbl_warehouse(i)->pim_GetRecord(txn, pk, warehouse_v));
#else
      tbl_warehouse(i)->GetRecord(txn, rc, Encode(str(Size(k)), k), warehouse_v);
#endif
      TryVerifyStrict(rc);

      const warehouse::value *v = Decode(warehouse_v, warehouse_temp);
      if (FLAGS_tpcc_numa_local) {
        ALWAYS_ASSERT(warehouses[(i-1)/ermia::config::numa_nodes] == *v);
      } else {
        ALWAYS_ASSERT(warehouses[i - 1] == *v);
      }

#ifndef NDEBUG
      checker::SanityCheckWarehouse(&k, v);
#endif
      TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
    }

    // pre-build supp-stock mapping table to boost tpc-ch queries
    if (loader_id == 0) {
      for (uint w = 1; w <= NumWarehouses(); w++) {
        for (uint i = 1; i <= NumItems(); i++) {
          supp_stock_map[w * i % 10000].push_back(std::make_pair(w, i));
        }
      }
    }
    LOG(INFO) << "Finished loading warehouse";
    LOG(INFO) << "  * total/average warehouse record length: "
         << warehouse_total_sz << "/" << (double(warehouse_total_sz) / double(n_warehouses)) << " bytes";
  }
};

class tpcc_item_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_item_loader(unsigned long seed, ermia::Engine *db,
                   const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                   const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions,
                   int loader_id = 0)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions), loader_id(loader_id) {}

 protected:
  int loader_id;
  virtual void load() {
    if (FLAGS_tpcc_numa_local) ALWAYS_ASSERT(loader_id == me->node);
    std::string obj_buf;
    uint64_t total_sz = 0;
    constexpr uint batchsize = 32;
    for (uint i_begin = 1; i_begin <= NumItems(); i_begin += batchsize) {
      arena->reset();
      ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
      const uint num = std::min<uint>(batchsize, NumItems() - i_begin + 1);
#if defined(OLTPIM)
      oltpim::request_insert reqs[batchsize];
#endif
      for (uint j = 0; j < num; ++j) {
        const item::key k(i_begin + j);

        item::value v;
        const std::string i_name = RandomStr(r, RandomNumber(r, 14, 24));
        v.i_name.assign(i_name);
        v.i_price = (float)RandomNumber(r, 100, 10000) / 100.0;
        const int len = RandomNumber(r, 26, 50);
        if (RandomNumber(r, 1, 100) > 10) {
          const std::string i_data = RandomStr(r, len);
          v.i_data.assign(i_data);
        } else {
          const int startOriginal = RandomNumber(r, 2, (len - 8));
          const std::string i_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" +
                                RandomStr(r, len - startOriginal - 7);
          v.i_data.assign(i_data);
        }
        v.i_im_id = RandomNumber(r, 1, 10000);

#ifndef NDEBUG
        checker::SanityCheckItem(&k, &v);
#endif
        const size_t sz = Size(v);
        total_sz += sz;
#if defined(OLTPIM)
        tbl_item(1+loader_id)->pim_InsertRecordBegin(txn, tpcc_key64::item(k), Encode(str(sz), v), &reqs[j]);
      }
      for (uint j = 0; j < num; ++j) {
        TryVerifyStrict(sync_wait_oltpim_coro(tbl_item(1+loader_id)->pim_InsertRecordEnd(txn, &reqs[j])));
#else
        if ((i_begin + j) * 100 <= NumItems() * (100 - FLAGS_tpcc_cold_item_pct)) {
#if defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
          TryVerifyStrict(sync_wait_coro(tbl_item(1+loader_id)->InsertRecord(
              txn, Encode(str(Size(k)), k),
              Encode(str(sz), v))));  // this table is shared, so any partition is OK
#else
          TryVerifyStrict(tbl_item(1+loader_id)->InsertRecord(
              txn, Encode(str(Size(k)), k),
              Encode(str(sz), v)));  // this table is shared, so any partition is OK
#endif
        } else {
#if defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
          TryVerifyStrict(sync_wait_coro(tbl_item(1+loader_id)->InsertColdRecord(
              txn, Encode(str(Size(k)), k),
              Encode(str(sz), v))));  // this table is shared, so any partition is OK
#else
          TryVerifyStrict(tbl_item(1+loader_id)->InsertColdRecord(
              txn, Encode(str(Size(k)), k),
              Encode(str(sz), v)));  // this table is shared, so any partition is OK
#endif
        }
#endif
      }
      TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
    }
    if (ermia::config::verbose) {
      LOG(INFO) << "Finished loading item";
      LOG(INFO) << "  * total/average item record length: "
           << total_sz << "/" << (double(total_sz) / double(NumItems())) << " bytes";
    }
  }
};

class tpcc_stock_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_stock_loader(unsigned long seed, ermia::Engine *db,
                    const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                    const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions,
                    ssize_t warehouse_id)
      : bench_loader(seed, db, open_tables),
        tpcc_worker_mixin(partitions),
        warehouse_id(warehouse_id) {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

 protected:
  virtual void load() {
    if (FLAGS_tpcc_numa_local) ALWAYS_ASSERT((warehouse_id-1) % ermia::config::numa_nodes == me->node);
    std::string obj_buf, obj_buf1;

    uint64_t stock_total_sz = 0, n_stocks = 0;
    const uint w_start =
        (warehouse_id == -1) ? 1 : static_cast<uint>(warehouse_id);
    const uint w_end = (warehouse_id == -1) ? NumWarehouses()
                                            : static_cast<uint>(warehouse_id);

    for (uint w = w_start; w <= w_end; w++) {
      constexpr uint batchsize = 32;
      for (uint i_begin = 1; i_begin <= NumItems(); i_begin += batchsize) {
        arena->reset();
        ermia::transaction *const txn = db->NewTransaction(0, *arena, txn_buf());
        const uint num = std::min<uint>(batchsize, NumItems() - i_begin + 1);
#if defined(OLTPIM)
        oltpim::request_insert reqs[2 * batchsize];
#endif
        for (uint j = 0; j < num; ++j) {
          const stock::key k(w, i_begin + j);
          const stock_data::key k_data(w, i_begin + j);

          stock::value v;
          v.s_quantity = RandomNumber(r, 10, 100);
          v.s_ytd = 0;
          v.s_order_cnt = 0;
          v.s_remote_cnt = 0;

          stock_data::value v_data;
          const int len = RandomNumber(r, 26, 50);
          if (RandomNumber(r, 1, 100) > 10) {
            const std::string s_data = RandomStr(r, len);
            v_data.s_data.assign(s_data);
          } else {
            const int startOriginal = RandomNumber(r, 2, (len - 8));
            const std::string s_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" +
                                  RandomStr(r, len - startOriginal - 7);
            v_data.s_data.assign(s_data);
          }
          v_data.s_dist_01.assign(RandomStr(r, 24));
          v_data.s_dist_02.assign(RandomStr(r, 24));
          v_data.s_dist_03.assign(RandomStr(r, 24));
          v_data.s_dist_04.assign(RandomStr(r, 24));
          v_data.s_dist_05.assign(RandomStr(r, 24));
          v_data.s_dist_06.assign(RandomStr(r, 24));
          v_data.s_dist_07.assign(RandomStr(r, 24));
          v_data.s_dist_08.assign(RandomStr(r, 24));
          v_data.s_dist_09.assign(RandomStr(r, 24));
          v_data.s_dist_10.assign(RandomStr(r, 24));

#ifndef NDEBUG
          checker::SanityCheckStock(&k);
#endif
          const size_t sz = Size(v);
          stock_total_sz += sz;
          n_stocks++;
#if defined(OLTPIM)
          const uint64_t pk = tpcc_key64::stock(k);
          tbl_stock(w)->pim_InsertRecordBegin(txn, pk, Encode(str(sz), v), &reqs[j]);
          tbl_stock_data(w)->pim_InsertRecordBegin(txn, pk, Encode(str(Size(v_data)), v_data), &reqs[batchsize + j]);
        }
        for (uint j = 0; j < num; ++j) {
          TryVerifyStrict(sync_wait_oltpim_coro(tbl_stock(w)->pim_InsertRecordEnd(txn, &reqs[j])));
          TryVerifyStrict(sync_wait_oltpim_coro(tbl_stock_data(w)->pim_InsertRecordEnd(txn, &reqs[batchsize + j])));
#elif defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
          TryVerifyStrict(sync_wait_coro(tbl_stock(w)->InsertRecord(txn, Encode(str(Size(k)), k),
                                                 Encode(str(sz), v))));
          TryVerifyStrict(
              sync_wait_coro(tbl_stock_data(w)->InsertRecord(txn, Encode(str(Size(k_data)), k_data),
                                        Encode(str(Size(v_data)), v_data))));
#else
          TryVerifyStrict(tbl_stock(w)->InsertRecord(txn, Encode(str(Size(k)), k),
                                                 Encode(str(sz), v)));
          TryVerifyStrict(
              tbl_stock_data(w)->InsertRecord(txn, Encode(str(Size(k_data)), k_data),
                                        Encode(str(Size(v_data)), v_data)));
#endif
        }
        TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
      }
    }
    if (warehouse_id == -1) {
      LOG(INFO) << "Finished loading stock";
      LOG(INFO) << "  * total/average stock record length: "
           << stock_total_sz << "/" << (double(stock_total_sz) / double(n_stocks)) << " bytes";
    } else {
      LOG(INFO) <<  "Finished loading stock (w=" << warehouse_id << ")";
      LOG(INFO) << "  * total/average stock (w=" << warehouse_id << ") record length: "
           << stock_total_sz << "/" << (double(stock_total_sz) / double(n_stocks)) << " bytes";
    }
  }

 private:
  ssize_t warehouse_id;
};

class tpcc_district_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_district_loader(unsigned long seed, ermia::Engine *db,
                       const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                       const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions,
                       int loader_id = 0)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions), loader_id(loader_id) {}

 protected:
  int loader_id;
  virtual void load() {
        if (FLAGS_tpcc_numa_local) ALWAYS_ASSERT(loader_id == me->node);
    std::string obj_buf;

    const ssize_t bsize = 10;
    uint64_t district_total_sz = 0, n_districts = 0;
    uint cnt = 0;
    for (uint w = 1; w <= NumWarehouses(); w++) {
      if (FLAGS_tpcc_numa_local && ((w-1)%ermia::config::numa_nodes != loader_id)) continue;
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++, cnt++) {
        arena->reset();
        ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
        const district::key k(w, d);

        district::value v;
        v.d_w_id = w;
        v.d_id = d;
        v.d_ytd = 30000;
        v.d_tax = (float)(RandomNumber(r, 0, 2000) / 10000.0);
        v.d_next_o_id = 3001;
        v.d_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
        v.d_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.d_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.d_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.d_state.assign(RandomStr(r, 3));
        v.d_zip.assign("123456789");

#ifndef NDEBUG
        checker::SanityCheckDistrict(&k, &v);
#endif
        const size_t sz = Size(v);
        district_total_sz += sz;
        n_districts++;
#if defined(OLTPIM)
        TryVerifyStrict(sync_wait_oltpim_coro(tbl_district(w)->pim_InsertRecord(txn, tpcc_key64::district(k), Encode(str(sz), v))));
#elif defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
        TryVerifyStrict(sync_wait_coro(tbl_district(w)->InsertRecord(txn, Encode(str(Size(k)), k),
                                                  Encode(str(sz), v))));
#else
        TryVerifyStrict(tbl_district(w)->InsertRecord(txn, Encode(str(Size(k)), k),
                                                  Encode(str(sz), v)));
#endif

        TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
      }
    }
    if (ermia::config::verbose) {
      LOG(INFO) << "Finished loading district";
      LOG(INFO) << "   * total/average district record length: "
           << district_total_sz << "/" << (double(district_total_sz) / double(n_districts)) << " bytes";
    }
  }
};

class tpcc_customer_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_customer_loader(unsigned long seed, ermia::Engine *db,
                       const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                       const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions,
                       ssize_t warehouse_id)
      : bench_loader(seed, db, open_tables),
        tpcc_worker_mixin(partitions),
        warehouse_id(warehouse_id) {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

 protected:
  virtual void load() {
    if (FLAGS_tpcc_numa_local) ALWAYS_ASSERT((warehouse_id-1) % ermia::config::numa_nodes == me->node);
    std::string obj_buf;

    const uint w_start =
        (warehouse_id == -1) ? 1 : static_cast<uint>(warehouse_id);
    const uint w_end = (warehouse_id == -1) ? NumWarehouses()
                                            : static_cast<uint>(warehouse_id);
    constexpr size_t batchsize = 32;
    uint64_t total_sz = 0;

    for (uint w = w_start; w <= w_end; w++) {
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
        for (uint c_begin = 1; c_begin <= NumCustomersPerDistrict(); c_begin += batchsize) {
          const uint num = std::min(batchsize, NumCustomersPerDistrict() - c_begin + 1);
          arena->reset();
          ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
#if defined(OLTPIM)
          uint64_t pk_idx[batchsize];
          oltpim::request_insert reqs[batchsize];
#endif
          for (uint j = 0; j < num; ++j) {
            const uint c = c_begin + j;
            const customer::key k(w, d, c);

            customer::value v;
            v.c_id = c;  // Put the c_id here in the tuple, needed by
                         // order-status later
            v.c_discount = (float)(RandomNumber(r, 1, 5000) / 10000.0);
            if (RandomNumber(r, 1, 100) <= 10)
              v.c_credit.assign("BC");
            else
              v.c_credit.assign("GC");

            if (c <= 1000)
              v.c_last.assign(GetCustomerLastName(c - 1));
            else
              v.c_last.assign(GetNonUniformCustomerLastNameLoad(r));

            v.c_first.assign(RandomStr(r, RandomNumber(r, 8, 16)));
            v.c_credit_lim = 50000;

            v.c_balance = -10;
            v.c_ytd_payment = 10;
            v.c_payment_cnt = 1;
            v.c_delivery_cnt = 0;

            v.c_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            v.c_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            v.c_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            v.c_state.assign(RandomStr(r, 3));
            v.c_zip.assign(RandomNStr(r, 4) + "11111");
            v.c_phone.assign(RandomNStr(r, 16));
            v.c_since = GetCurrentTimeMillis();
            v.c_middle.assign("OE");
            v.c_data.assign(RandomStr(r, RandomNumber(r, 300, 500)));

#ifndef NDEBUG
            checker::SanityCheckCustomer(&k, &v);
#endif
            const size_t sz = Size(v);
            total_sz += sz;

            // customer name index
            const customer_name_idx::key k_idx(
                k.c_w_id, k.c_d_id, v.c_last.str(true), v.c_first.str(true));
            
            // index structure is:
            // (c_w_id, c_d_id, c_last, c_first) -> OID

#if defined(OLTPIM)
            tbl_customer(w)->pim_InsertRecordBegin(txn, tpcc_key64::customer(k), Encode(str(sz), v), &reqs[j]);
            pk_idx[j] = tpcc_key64::customer_name_idx(k_idx);
          }
          for (uint j = 0; j < num; ++j) {
            uint64_t c_oid = 0;
            TryVerifyStrict(sync_wait_oltpim_coro(tbl_customer(w)->pim_InsertRecordEnd(txn, &reqs[j], &c_oid)));
            tbl_customer_name_idx(w)->pim_InsertOIDBegin(txn, pk_idx[j], c_oid, &reqs[j]);
          }
          for (uint j = 0; j < num; ++j) {
            TryVerifyStrict(sync_wait_oltpim_coro(tbl_customer_name_idx(w)->pim_InsertOIDEnd(txn, &reqs[j])));
#else
            ermia::OID c_oid = 0;  // Get the OID and put in customer_name_idx later
            if (j * 100 / num <= (100 - FLAGS_tpcc_cold_customer_pct)) {
#if defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
              TryVerifyStrict(sync_wait_coro(tbl_customer(w)->InsertRecord(
                  txn, Encode(str(Size(k)), k), Encode(str(sz), v), &c_oid)));
#else
              TryVerifyStrict(tbl_customer(w)->InsertRecord(
                  txn, Encode(str(Size(k)), k), Encode(str(sz), v), &c_oid));
#endif
            } else {
#if defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
              TryVerifyStrict(sync_wait_coro(tbl_customer(w)->InsertColdRecord(
                  txn, Encode(str(Size(k)), k), Encode(str(sz), v), &c_oid)));
#else
              TryVerifyStrict(tbl_customer(w)->InsertColdRecord(
                  txn, Encode(str(Size(k)), k), Encode(str(sz), v), &c_oid));
#endif
            }

#if defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
            TryVerifyStrict(sync_wait_coro(tbl_customer_name_idx(w)->InsertOID(
                txn, Encode(str(Size(k_idx)), k_idx), c_oid)));
#else
            TryVerifyStrict(tbl_customer_name_idx(w)->InsertOID(
                txn, Encode(str(Size(k_idx)), k_idx), c_oid));
#endif
#endif // !defined(OLTPIM)
          }
          TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));

          arena->reset();
          txn = db->NewTransaction(0, *arena, txn_buf());
          for (uint j = 0; j < num; ++j) {
            const uint c = c_begin + j;
            history::key k_hist;
            k_hist.h_c_id = c;
            k_hist.h_c_d_id = d;
            k_hist.h_c_w_id = w;
            k_hist.h_d_id = d;
            k_hist.h_w_id = w;
            k_hist.h_date = GetCurrentTimeMillis();

            history::value v_hist;
            v_hist.h_amount = 10;
            v_hist.h_data.assign(RandomStr(r, RandomNumber(r, 10, 24)));

#if defined(OLTPIM)
            tbl_history(w)->pim_InsertRecordBegin(txn, tpcc_key64::history(k_hist), Encode(str(Size(v_hist)), v_hist), &reqs[j]);
          }
          for (uint j = 0; j < num; ++j) {
            TryVerifyStrict(sync_wait_oltpim_coro(tbl_history(w)->pim_InsertRecordEnd(txn, &reqs[j])));
#else
#if defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
            TryVerifyStrict(
                sync_wait_coro(tbl_history(w)->InsertRecord(txn, Encode(str(Size(k_hist)), k_hist),
                                       Encode(str(Size(v_hist)), v_hist))));
#else
            TryVerifyStrict(
                tbl_history(w)->InsertRecord(txn, Encode(str(Size(k_hist)), k_hist),
                                       Encode(str(Size(v_hist)), v_hist)));
#endif
#endif // !defined(OLTPIM)
          }
          TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
        }
      }
    }
    if (warehouse_id == -1) {
      LOG(INFO) << "Finished loading customer";
      LOG(INFO) << "   * total/average customer record length: "
           << total_sz << "/" << (double(total_sz) /
               double(NumWarehouses() * NumDistrictsPerWarehouse() *
                      NumCustomersPerDistrict())) << " bytes ";
    } else {
      LOG(INFO) << "Finished loading customer (w=" << warehouse_id << ")";
      LOG(INFO) << "   * total/average customer (w=" << warehouse_id << ") record length: "
           << total_sz << "/" << (double(total_sz) /
               double(NumWarehouses() * NumDistrictsPerWarehouse() *
                      NumCustomersPerDistrict())) << " bytes ";
    }
  }

 private:
  ssize_t warehouse_id;
};

class tpcc_order_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_order_loader(unsigned long seed, ermia::Engine *db,
                    const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                    const std::map<std::string, std::vector<ermia::OrderedIndex *>> &partitions,
                    ssize_t warehouse_id)
      : bench_loader(seed, db, open_tables),
        tpcc_worker_mixin(partitions),
        warehouse_id(warehouse_id) {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

 protected:
  size_t NumOrderLinesPerCustomer() { return RandomNumber(r, 5, 15); }

  virtual void load() {
    if (FLAGS_tpcc_numa_local) ALWAYS_ASSERT((warehouse_id-1) % ermia::config::numa_nodes == me->node);
    std::string obj_buf;

    uint64_t order_line_total_sz = 0, n_order_lines = 0;
    uint64_t oorder_total_sz = 0, n_oorders = 0;
    uint64_t new_order_total_sz = 0, n_new_orders = 0;

    const uint w_start =
        (warehouse_id == -1) ? 1 : static_cast<uint>(warehouse_id);
    const uint w_end = (warehouse_id == -1) ? NumWarehouses()
                                            : static_cast<uint>(warehouse_id);
    constexpr size_t batchsize = 15 * 3;
    static_assert(batchsize % 15 == 0); // to reuse buffers for order_line insertion
    constexpr size_t ol_batchsize = batchsize / 15;

    for (uint w = w_start; w <= w_end; w++) {
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
        std::set<uint> c_ids_s;
        std::vector<uint> c_ids;
        while (c_ids.size() != NumCustomersPerDistrict()) {
          const auto x = (r.next() % NumCustomersPerDistrict()) + 1;
          if (c_ids_s.count(x)) continue;
          c_ids_s.insert(x);
          c_ids.emplace_back(x);
        }
        for (uint c_begin = 1; c_begin <= NumCustomersPerDistrict(); c_begin += batchsize) {
          const size_t num = std::min(batchsize, NumCustomersPerDistrict() - c_begin + 1);
          arena->reset();
          ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
#if defined(OLTPIM)
          oltpim::request_insert reqs[batchsize];
#endif
          struct {
            uint32_t o_entry_d;
            int8_t o_ol_cnt;
          } v_oo_reuse[batchsize];

          for (uint j = 0; j < num; ++j) {
            const uint c = c_begin + j;
            const oorder::key k_oo(w, d, c);

            oorder::value v_oo;
            v_oo.o_w_id = w;
            v_oo.o_d_id = d;
            v_oo.o_id = c;
            v_oo.o_c_id = c_ids[c - 1];
            if (k_oo.o_id < 2101)
              v_oo.o_carrier_id = RandomNumber(r, 1, 10);
            else
              v_oo.o_carrier_id = 0;
            v_oo.o_ol_cnt = NumOrderLinesPerCustomer();
            v_oo.o_all_local = 1;
            v_oo.o_entry_d = GetCurrentTimeMillis();
            v_oo_reuse[j].o_entry_d = v_oo.o_entry_d;
            v_oo_reuse[j].o_ol_cnt = v_oo.o_ol_cnt;

#ifndef NDEBUG
            checker::SanityCheckOOrder(&k_oo, &v_oo);
#endif
            const size_t sz = Size(v_oo);
            oorder_total_sz += sz;
            n_oorders++;
#if defined(OLTPIM)
            tbl_oorder(w)->pim_InsertRecordBegin(txn, tpcc_key64::oorder(k_oo), Encode(str(sz), v_oo), &reqs[j]);
          }
          for (uint j = 0; j < num; ++j) {
            const uint c = c_begin + j;
            uint64_t v_oo_oid = 0;
            TryVerifyStrict(sync_wait_oltpim_coro(tbl_oorder(w)->pim_InsertRecordEnd(txn, &reqs[j], &v_oo_oid)));

#else
            ermia::OID v_oo_oid = 0;  // Get the OID and put it in oorder_c_id_idx later
#if defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
            TryVerifyStrict(
                sync_wait_coro(tbl_oorder(w)->InsertRecord(txn, Encode(str(Size(k_oo)), k_oo),
                                      Encode(str(sz), v_oo), &v_oo_oid)));
#else
            TryVerifyStrict(
                tbl_oorder(w)->InsertRecord(txn, Encode(str(Size(k_oo)), k_oo),
                                      Encode(str(sz), v_oo), &v_oo_oid));
#endif
#endif // !defined(OLTPIM)

            const oorder_c_id_idx::key k_oo_idx(w, d, c_ids[c - 1], c);
#if defined(OLTPIM)
            const uint64_t pk_oo_idx = tpcc_key64::oorder_c_id_idx(k_oo_idx);
            tbl_oorder_c_id_idx(w)->pim_InsertOIDBegin(txn, pk_oo_idx, v_oo_oid, &reqs[j]);
          }
          for (uint j = 0; j < num; ++j) {
            const uint c = c_begin + j;
            TryVerifyStrict(sync_wait_oltpim_coro(tbl_oorder_c_id_idx(w)->pim_InsertOIDEnd(txn, &reqs[j])));
#else
#if defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
            TryVerifyStrict(sync_wait_coro(tbl_oorder_c_id_idx(w)->InsertOID(
                txn, Encode(str(Size(k_oo_idx)), k_oo_idx), v_oo_oid)));
#else
            TryVerifyStrict(tbl_oorder_c_id_idx(w)->InsertOID(
                txn, Encode(str(Size(k_oo_idx)), k_oo_idx), v_oo_oid));
#endif
#endif // !defined(OLTPIM)

            if (c >= 2101) {
              const new_order::key k_no(w, d, c);
              const new_order::value v_no(w, d, c);

#ifndef NDEBUG
              checker::SanityCheckNewOrder(&k_no);
#endif
              const size_t sz = Size(v_no);
              new_order_total_sz += sz;
              n_new_orders++;
#if defined(OLTPIM)
              tbl_new_order(w)->pim_InsertRecordBegin(txn, tpcc_key64::new_order(k_no), Encode(str(sz), v_no), &reqs[j]);
            }
          }
          for (uint j = 0; j < num; ++j) {
            const uint c = c_begin + j;
            if (c >= 2101) {
              TryVerifyStrict(sync_wait_oltpim_coro(tbl_new_order(w)->pim_InsertRecordEnd(txn, &reqs[j])));
#else
#if defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
              TryVerifyStrict(sync_wait_coro(tbl_new_order(w)->InsertRecord(
                  txn, Encode(str(Size(k_no)), k_no), Encode(str(sz), v_no))));
#else
              TryVerifyStrict(tbl_new_order(w)->InsertRecord(
                  txn, Encode(str(Size(k_no)), k_no), Encode(str(sz), v_no)));
#endif
#endif // !defined(OLTPIM)
            }
          }
          TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));

          for (uint j_out = 0; j_out < num; j_out += ol_batchsize) {
            const uint num_oos = std::min<uint>(ol_batchsize, num - j_out);
            arena->reset();
            txn = db->NewTransaction(0, *arena, txn_buf());
            for (uint j_in = 0; j_in < num_oos; ++j_in) {
              const uint j = j_out + j_in;
              const uint c = c_begin + j;
              for (uint l = 0; l < uint(v_oo_reuse[j].o_ol_cnt); ++l) {
                const uint batch_jdx = j_in * 15 + l;
                const order_line::key k_ol(w, d, c, l + 1);

                order_line::value v_ol;
                v_ol.ol_w_id = w;
                v_ol.ol_d_id = d;
                v_ol.ol_o_id = c;
                v_ol.ol_number = l + 1;
                v_ol.ol_i_id = RandomNumber(r, 1, 100000);
                if (k_ol.ol_o_id < 2101) {
                  v_ol.ol_delivery_d = v_oo_reuse[j].o_entry_d;
                  v_ol.ol_amount = 0;
                } else {
                  v_ol.ol_delivery_d = 0;
                  // random within [0.01 .. 9,999.99]
                  v_ol.ol_amount = (float)(RandomNumber(r, 1, 999999) / 100.0);
                }

                v_ol.ol_supply_w_id = k_ol.ol_w_id;
                v_ol.ol_quantity = 5;
                // v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
                // v_ol.ol_dist_info = RandomStr(r, 24);

#ifndef NDEBUG
                checker::SanityCheckOrderLine(&k_ol, &v_ol);
#endif
                const size_t sz = Size(v_ol);
                order_line_total_sz += sz;
                n_order_lines++;

#if defined(OLTPIM)
                tbl_order_line(w)->pim_InsertRecordBegin(txn, tpcc_key64::order_line(k_ol), Encode(str(sz), v_ol), &reqs[batch_jdx]);
              }
            }
            for (uint j_in = 0; j_in < num_oos; ++j_in) {
              const uint j = j_out + j_in;
              for (uint l = 0; l < uint(v_oo_reuse[j].o_ol_cnt); ++l) {
                const uint batch_jdx = j_in * 15 + l;
                TryVerifyStrict(sync_wait_oltpim_coro(tbl_order_line(w)->pim_InsertRecordEnd(txn, &reqs[batch_jdx])));
#else
#if defined(NESTED_COROUTINE) || defined(HYBRID_COROUTINE)
                TryVerifyStrict(sync_wait_coro(tbl_order_line(w)->InsertRecord(
                    txn, Encode(str(Size(k_ol)), k_ol), Encode(str(sz), v_ol))));
#else
                TryVerifyStrict(tbl_order_line(w)->InsertRecord(
                    txn, Encode(str(Size(k_ol)), k_ol), Encode(str(sz), v_ol)));
#endif
#endif // !defined(OLTPIM)
              }
            }
            TryVerifyStrict(COMMIT_SYNC(ENGINE_COMMIT(db, txn)));
          }
        }
      }
    }

    if (ermia::config::verbose) {
      if (warehouse_id == -1) {
        LOG(INFO) << "finished loading order";
        LOG(INFO) << "  * average order_line record length: "
             << (double(order_line_total_sz) / double(n_order_lines))
             << " bytes";
        LOG(INFO) << "  * average oorder record length: "
             << (double(oorder_total_sz) / double(n_oorders)) << " bytes";
        LOG(INFO) << "   * average new_order record length: "
             << (double(new_order_total_sz) / double(n_new_orders)) << " bytes";
      } else {
        LOG(INFO) << " Finished loading order (w=" << warehouse_id << ")";
        LOG(INFO) << "  * total/average order_line (w=" << warehouse_id << ") record length: "
             << order_line_total_sz << "/" << (double(order_line_total_sz) / double(n_order_lines)) << " bytes";
        LOG(INFO) << "  * total/average oorder record length: "
             << oorder_total_sz << "/" << (double(oorder_total_sz) / double(n_oorders)) << " bytes";
        LOG(INFO) << "   * total/average new_order record length: "
             << new_order_total_sz << "/" << (double(new_order_total_sz) / double(n_new_orders)) << " bytes";
      }
    }
  }

 private:
  ssize_t warehouse_id;
};

// explicitly copies keys, because btree::search_range_call() interally
// re-uses a single string to pass keys (so using standard string assignment
// will force a re-allocation b/c of shared ref-counting)
//
// this isn't done for values, because all values are read-only in a
// multi-version
// system. ermia::varstrs for values only point to the real data in the database, but
// still we need to allocate a ermia::varstr header for each value. Internally it's
// just a ermia::varstr in the stack.
template <size_t N>
class static_limit_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  // XXX: push ignore_key into lower layer
  static_limit_callback(ermia::str_arena *arena, bool ignore_key)
      : n(0), arena(arena), ignore_key(ignore_key) {
    static_assert(N > 0, "xx");
    values.reserve(N);
  }

  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    ASSERT(n < N);
    ermia::varstr *pv = arena->next(0);  // header only
    pv->p = value.p;
    pv->l = value.l;
    if (ignore_key) {
      values.emplace_back(nullptr, pv);
    } else {
      ermia::varstr *const s_px = arena->next(keylen);
      ASSERT(s_px);
      s_px->copy_from(keyp, keylen);
      values.emplace_back(s_px, pv);
    }
    return ++n < N;
  }

  inline size_t size() const { return values.size(); }

  typedef std::pair<const ermia::varstr *, const ermia::varstr *> kv_pair;
  typename std::vector<kv_pair> values;

 private:
  size_t n;
  ermia::str_arena *arena;
  bool ignore_key;
};


class credit_check_order_line_scan_callback
    : public ermia::OrderedIndex::ScanCallback {
 public:
  credit_check_order_line_scan_callback() : sum(0) {}
  inline virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    MARK_REFERENCED(keyp);
    MARK_REFERENCED(keylen);
    order_line::value v_ol_temp;
    const order_line::value *val = Decode(value, v_ol_temp);
    sum += val->ol_amount;
    return true;
  }
  double sum;
};

class credit_check_order_scan_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  credit_check_order_scan_callback(ermia::str_arena *arena) : _arena(arena) {}
  inline virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    MARK_REFERENCED(value);
    ermia::varstr *const k = _arena->next(keylen);
    ASSERT(k);
    k->copy_from(keyp, keylen);
    output.emplace_back(k);
    return true;
  }
  std::vector<ermia::varstr *> output;
  ermia::str_arena *_arena;
};

class order_line_nop_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  order_line_nop_callback() : n(0) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    MARK_REFERENCED(keylen);
    MARK_REFERENCED(keyp);
    ASSERT(keylen == sizeof(order_line::key));
    order_line::value v_ol_temp;
    const order_line::value *v_ol = Decode(value, v_ol_temp);
#ifndef NDEBUG
    order_line::key k_ol_temp;
    const order_line::key *k_ol = Decode(keyp, k_ol_temp);
    checker::SanityCheckOrderLine(k_ol, v_ol);
#endif
    ++n;
    return true;
  }
  size_t n;
};

class latest_key_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  latest_key_callback(ermia::varstr &k, int32_t limit = -1)
      : limit(limit), n(0), k(&k) {
    ALWAYS_ASSERT(limit == -1 || limit > 0);
  }

  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    MARK_REFERENCED(value);
    ASSERT(limit == -1 || n < limit);
    k->copy_from(keyp, keylen);
    ++n;
    return (limit == -1) || (n < limit);
  }

  inline size_t size() const { return n; }
  inline ermia::varstr &kstr() { return *k; }

 private:
  int32_t limit;
  int32_t n;
  ermia::varstr *k;
};


class order_line_scan_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  order_line_scan_callback() : n(0) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    MARK_REFERENCED(keyp);
    MARK_REFERENCED(keylen);
    ASSERT(keylen == sizeof(order_line::key));
    order_line::value v_ol_temp;
    const order_line::value *v_ol = Decode(value, v_ol_temp);

#ifndef NDEBUG
    order_line::key k_ol_temp;
    const order_line::key *k_ol = Decode(keyp, k_ol_temp);
    checker::SanityCheckOrderLine(k_ol, v_ol);
#endif

    s_i_ids[v_ol->ol_i_id] = 1;
    n++;
    return true;
  }
  size_t n;
  std::unordered_map<uint, bool> s_i_ids;
};

class new_order_scan_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  new_order_scan_callback() : k_no(0) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    MARK_REFERENCED(keylen);
    MARK_REFERENCED(value);
    ASSERT(keylen == sizeof(new_order::key));
    //ASSERT(value.size() == sizeof(new_order::value));
    k_no = Decode(keyp, k_no_temp);
#ifndef NDEBUG
    new_order::value v_no_temp;
    const new_order::value *v_no = Decode(value, v_no_temp);
    checker::SanityCheckNewOrder(k_no);
#endif
    return false;
  }
  inline const new_order::key *get_key() const { return k_no; }

 private:
  new_order::key k_no_temp;
  const new_order::key *k_no;
};

void tpcc_parse_options();

template <class WorkerType>
class tpcc_bench_runner : public bench_runner {
 private:
  static bool IsTableReadOnly(const char *name) {
    return strcmp("item", name) == 0;
  }

  static bool IsTableAppendOnly(const char *name) {
    return strcmp("history", name) == 0 || strcmp("oorder_c_id_idx", name) == 0;
  }

  static std::vector<ermia::OrderedIndex *> OpenIndexes(const char *name) {
    const bool is_read_only = IsTableReadOnly(name);
    const bool is_append_only = IsTableAppendOnly(name);
    const std::string s_name(name);
    std::vector<ermia::OrderedIndex *> ret(NumWarehouses());
    if (FLAGS_tpcc_enable_separate_tree_per_partition && !is_read_only) {
      ALWAYS_ASSERT(!FLAGS_tpcc_numa_local);
      if (NumWarehouses() <= ermia::config::worker_threads) {
        for (size_t i = 0; i < NumWarehouses(); i++) {
          ret[i] = ermia::TableDescriptor::GetIndex(s_name + "_" + std::to_string(i));
          ALWAYS_ASSERT(ret[i]);
        }
      } else {
        const unsigned nwhse_per_partition =
            NumWarehouses() / ermia::config::worker_threads;
        for (size_t partid = 0; partid < ermia::config::worker_threads; partid++) {
          const unsigned wstart = partid * nwhse_per_partition;
          const unsigned wend = (partid + 1 == ermia::config::worker_threads)
                                    ? NumWarehouses()
                                    : (partid + 1) * nwhse_per_partition;
          ermia::OrderedIndex *idx =
              ermia::TableDescriptor::GetIndex(s_name + "_" + std::to_string(partid));
          ALWAYS_ASSERT(idx);
          for (size_t i = wstart; i < wend; i++) {
            ret[i] = idx;
          }
        }
      }
    } else {
      if (!FLAGS_tpcc_numa_local) {
        ermia::OrderedIndex *idx = ermia::TableDescriptor::GetIndex(s_name);
        ALWAYS_ASSERT(idx);
        for (size_t i = 0; i < NumWarehouses(); i++) {
          ret[i] = idx;
        }
      }
      else {
        for (size_t i = 0; i < NumWarehouses(); i++) {
          int numa_id = (int)(i % ermia::config::numa_nodes);
          ret[i] = ermia::TableDescriptor::GetIndex(s_name + "_" + std::to_string(numa_id));
        }
      }
    }
    return ret;
  }

  // Create table and primary index (same name) or a secondary index if
  // primary_idx_name isn't nullptr
  static void RegisterIndex(ermia::Engine *db, const char *table_name,
                            const char *index_name, bool is_primary) {
    const bool is_read_only = IsTableReadOnly(index_name);

    // A labmda function to be executed by an sm-thread
    auto register_index = [=](char *) {
      if (FLAGS_tpcc_enable_separate_tree_per_partition && !is_read_only) {
        ALWAYS_ASSERT(!FLAGS_tpcc_numa_local); // not implemented
        if (NumWarehouses() <= ermia::config::worker_threads) {
          for (size_t i = 0; i < NumWarehouses(); i++) {
            if (!is_primary) {
              // Secondary index
              db->CreateMasstreeSecondaryIndex(table_name, std::string(index_name));
            } else {
              db->CreateTable(table_name);
              db->CreateMasstreePrimaryIndex(table_name, std::string(index_name));
            }
          }
        } else {
          const unsigned nwhse_per_partition =
              NumWarehouses() / ermia::config::worker_threads;
          for (size_t partid = 0; partid < ermia::config::worker_threads; partid++) {
            const unsigned wstart = partid * nwhse_per_partition;
            const unsigned wend = (partid + 1 == ermia::config::worker_threads)
                                      ? NumWarehouses()
                                      : (partid + 1) * nwhse_per_partition;
            if (!is_primary) {
              auto s_primary_name = std::string(index_name) + "_" + std::to_string(partid);
              db->CreateMasstreeSecondaryIndex(table_name, s_primary_name);
            } else {
              db->CreateTable(table_name);
              auto ss_name = std::string(index_name) + "_" + std::to_string(partid);
              db->CreateMasstreePrimaryIndex(table_name, ss_name);
            }
          }
        }
      } else {
        if (!FLAGS_tpcc_numa_local) {
          if (!is_primary) {
            db->CreateMasstreeSecondaryIndex(table_name, index_name);
          } else {
            db->CreateTable(table_name);
            db->CreateMasstreePrimaryIndex(table_name, std::string(index_name));
          }
        }
        else {
          ALWAYS_ASSERT(ermia::config::numa_spread);
          for (int i = 0; i < ermia::config::numa_nodes; ++i) {
            std::string tname = std::string(table_name) + "_" + std::to_string(i);
            std::string iname = std::string(index_name) + "_" + std::to_string(i);
            if (!is_primary) {
              db->CreateMasstreeSecondaryIndex(tname.c_str(), iname);
            } else {
              db->CreateTable(tname.c_str());
              db->CreateMasstreePrimaryIndex(tname.c_str(), iname);
            }
          }
        }
      }
    };

    ermia::thread::Thread *thread = ermia::thread::GetThread(ermia::thread::CoreType::PHYSICAL);
    ALWAYS_ASSERT(thread);
    thread->StartTask(register_index);
    thread->Join();
    ermia::thread::PutThread(thread);
  }

 public:
  tpcc_bench_runner(ermia::Engine *db) : bench_runner(db) {
    // Register all tables and indexes with the engine
    RegisterIndex(db, "customer",   "customer",         true);
    RegisterIndex(db, "customer",   "customer_name_idx",false);
    RegisterIndex(db, "district",   "district",         true);
    RegisterIndex(db, "history",    "history",          true);
    RegisterIndex(db, "item",       "item",             true);
    RegisterIndex(db, "new_order",  "new_order",        true);
    RegisterIndex(db, "oorder",     "oorder",           true);
    RegisterIndex(db, "oorder",     "oorder_c_id_idx",  false);
    RegisterIndex(db, "order_line", "order_line",       true);
    RegisterIndex(db, "stock",      "stock",            true);
    RegisterIndex(db, "stock_data", "stock_data",       true);
    RegisterIndex(db, "nation",     "nation",           true);
    RegisterIndex(db, "region",     "region",           true);
    RegisterIndex(db, "supplier",   "supplier",         true);
    RegisterIndex(db, "warehouse",  "warehouse",        true);
#if defined(OLTPIM)
#define tpcc_tables_helper(f) \
f(customer); f(customer_name_idx); f(district); f(history); f(item); f(new_order); \
f(oorder); f(oorder_c_id_idx); f(order_line); f(stock); f(stock_data); f(warehouse);
    if (!FLAGS_tpcc_numa_local) {
#define partition_interval_set(table) \
  ermia::pim::set_index_partition_interval(#table, \
    tpcc_key64::table##_bits, false, 0)
      tpcc_tables_helper(partition_interval_set)
#undef partition_interval_set
    }
    else {
      for (int numa_id = 0; numa_id < ermia::config::numa_nodes; ++numa_id) {
#define partition_interval_set(table) \
  ermia::pim::set_index_partition_interval((std::string(#table) + "_" + std::to_string(numa_id)).c_str(), \
    tpcc_key64::table##_bits, true, numa_id)
        tpcc_tables_helper(partition_interval_set)
#undef partition_interval_set
      }
    }
#undef tpcc_tables_helper
    ermia::pim::finalize_index_setup();
#endif
  }

  virtual void prepare(char *) {
#define OPEN_TABLESPACE_X(x) partitions[#x] = OpenIndexes(#x);

    TPCC_TABLE_LIST(OPEN_TABLESPACE_X);

#undef OPEN_TABLESPACE_X

    for (auto &t : partitions) {
      auto v = unique_filter(t.second);
      for (size_t i = 0; i < v.size(); i++)
        open_tables[t.first + "_" + std::to_string(i)] = v[i];
    }

    if (FLAGS_tpcc_new_order_fast_id_gen) {
      void *const px = memalign(
          CACHELINE_SIZE, sizeof(util::aligned_padded_elem<std::atomic<uint64_t>>) *
                              NumWarehouses() * NumDistrictsPerWarehouse());
      g_district_ids =
          reinterpret_cast<util::aligned_padded_elem<std::atomic<uint64_t>> *>(px);
      for (size_t i = 0; i < NumWarehouses() * NumDistrictsPerWarehouse(); i++)
        new (&g_district_ids[i]) std::atomic<uint64_t>(3001);
    }
    if (FLAGS_tpcc_atomic_ytd) {
      void *const pw = memalign(
          CACHELINE_SIZE, sizeof(util::aligned_padded_elem<std::atomic<float>>) *
                              NumWarehouses());
      void *const pd = memalign(
          CACHELINE_SIZE, sizeof(util::aligned_padded_elem<std::atomic<float>>) *
                              NumWarehouses() * NumDistrictsPerWarehouse());
      g_warehouse_ytds =
          reinterpret_cast<util::aligned_padded_elem<std::atomic<float>> *>(pw);
      g_district_ytds =
          reinterpret_cast<util::aligned_padded_elem<std::atomic<float>> *>(pd);
      for (size_t i = 0; i < NumWarehouses(); i++)
        new (&g_warehouse_ytds[i]) std::atomic<float>(300000);
      for (size_t i = 0; i < NumWarehouses() * NumDistrictsPerWarehouse(); i++)
        new (&g_district_ytds[i]) std::atomic<float>(300000);
    }
  }

 protected:
  virtual std::vector<bench_loader *> make_loaders() {
    std::vector<bench_loader *> ret;
    if (!FLAGS_tpcc_numa_local) {
      ret.push_back(new tpcc_warehouse_loader(9324, db, open_tables, partitions));
      //ret.push_back(new tpcc_nation_loader(1512, db, open_tables, partitions));
      //ret.push_back(new tpcc_region_loader(789121, db, open_tables, partitions));
      //ret.push_back(
      //    new tpcc_supplier_loader(51271928, db, open_tables, partitions));
      ret.push_back(new tpcc_item_loader(235443, db, open_tables, partitions));
      if (ermia::config::parallel_loading) {
        util::fast_random r(89785943);
        for (uint i = 1; i <= NumWarehouses(); i++)
          ret.push_back(
              new tpcc_stock_loader(r.next(), db, open_tables, partitions, i));
      } else {
        ret.push_back(
            new tpcc_stock_loader(89785943, db, open_tables, partitions, -1));
      }
      ret.push_back(
          new tpcc_district_loader(129856349, db, open_tables, partitions));
      if (ermia::config::parallel_loading) {
        util::fast_random r(923587856425);
        for (uint i = 1; i <= NumWarehouses(); i++)
          ret.push_back(
              new tpcc_customer_loader(r.next(), db, open_tables, partitions, i));
      } else {
        ret.push_back(new tpcc_customer_loader(923587856425, db, open_tables,
                                               partitions, -1));
      }
      if (ermia::config::parallel_loading) {
        util::fast_random r(2343352);
        for (uint i = 1; i <= NumWarehouses(); i++)
          ret.push_back(
              new tpcc_order_loader(r.next(), db, open_tables, partitions, i));
      } else {
        ret.push_back(
            new tpcc_order_loader(2343352, db, open_tables, partitions, -1));
      }
    }
    else {
      ALWAYS_ASSERT(ermia::config::parallel_loading);
      util::fast_random r(923587856425);
      for (int i = 0; i < ermia::config::numa_nodes; ++i)
        ret.push_back(new tpcc_warehouse_loader(r.next(), db, open_tables, partitions, i));
      for (int i = 0; i < ermia::config::numa_nodes; ++i)
        ret.push_back(new tpcc_item_loader(r.next(), db, open_tables, partitions, i));
      for (int i = 1; i <= NumWarehouses(); ++i)
        ret.push_back(new tpcc_stock_loader(r.next(), db, open_tables, partitions, i));
      for (int i = 0; i < ermia::config::numa_nodes; ++i)
        ret.push_back(new tpcc_district_loader(r.next(), db, open_tables, partitions, i));
      for (int i = 1; i <= NumWarehouses(); ++i)
        ret.push_back(new tpcc_customer_loader(r.next(), db, open_tables, partitions, i));
      for (int i = 1; i <= NumWarehouses(); ++i)
        ret.push_back(new tpcc_order_loader(r.next(), db, open_tables, partitions, i));
    }
    return ret;
  }

  virtual std::vector<bench_worker *> make_workers() {
    util::fast_random r(23984543);
    std::vector<bench_worker *> ret;
    if (FLAGS_tpcc_numa_local) {
      // i-th worker thread's j-th coroutine's home_warehouse_id is (i + j * worker_threads)
      // Because (i % numa_nodes) == numa_id, in order to make all home_warehouse_id to be numa_local,
      // (i + j * worker_threads) % numa_nodes == (i % numa_nodes) should be hold: below!
      ALWAYS_ASSERT(ermia::config::worker_threads % ermia::config::numa_nodes == 0);
    }
    for (size_t i = 0; i < ermia::config::worker_threads; i++)
      ret.push_back(new WorkerType(i, r.next(), db, open_tables, partitions,
                                  &barrier_a, &barrier_b));
    return ret;
  }

 private:
  std::map<std::string, std::vector<ermia::OrderedIndex *>> partitions;
};
