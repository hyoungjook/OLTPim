#pragma once
#include <algorithm>
#include <cmath>
#include <cstdint>
/**
 * Histogram counter.
 * Used to measure P99 latency.
 */

namespace ermia {

struct histogram_counter {
  static constexpr int MAX_CNTS = 30;
  // cnt[i]: count vals with (2^(i)) - (2^(i+1)-1)
  // if val >= 2^MAX_CNTS, goes to cnt[MAX_CNTS-1]
  uint64_t cnt[MAX_CNTS] = {0,};
  void add(uint64_t val) {
    const int index =
      (val > 0) ? (
        std::min<int>(
          std::log2(val),
          MAX_CNTS - 1
        )
      ) : 0;
    ++cnt[index];
  }
  histogram_counter& operator+=(const histogram_counter& other) {
    for (int i = 0; i < MAX_CNTS; i++) cnt[i] += other.cnt[i];
    return *this;
  }
  histogram_counter& operator-=(const histogram_counter& other) {
    for (int i = 0; i < MAX_CNTS; i++) cnt[i] -= other.cnt[i];
    return *this;
  }
  double pN(int n) { // ex. n=99 for p99 latency
    ASSERT(0 < n && n < 100);
    uint64_t pn_cnt = 0;
    for (int i = 0; i < MAX_CNTS; i++) {
      pn_cnt += cnt[i];
    }
    pn_cnt = (uint64_t)((double)pn_cnt / 100.0 * n);
    uint64_t accum_cnt = 0;
    for (int i = 0; i < MAX_CNTS; i++) {
      uint64_t bucket_cnt = cnt[i];
      if (accum_cnt + bucket_cnt >= pn_cnt) {
        uint64_t delta = pn_cnt - accum_cnt;
        double eff_i = (double)i + (double)delta / bucket_cnt;
        return std::exp2(eff_i);
      }
      accum_cnt += bucket_cnt;
    }
    return -1.0;
  }
};

}
