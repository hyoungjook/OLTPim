#pragma once

#include "tpcc.h"
#include <assert.h>

namespace tpcc_key64 {
// Embeds key of all tables into uint64 number

static inline uint64_t customer(const customer::key &k) {
  // w_id, 4b d_id, 20b c_id
  return (((uint64_t)k.c_w_id) << 24) | (((uint64_t)k.c_d_id) << 20) | (((uint64_t)k.c_id));
}

static constexpr uint64_t customer_name_idx_interval = 0x40000000UL;
static uint64_t customer_name_idx(const customer_name_idx::key &k,
    bool min_c_first = false, bool max_c_first = false) {
  // w_id, 4b d_id, 10b encoded c_last, 6b x (first 5 alnums of c_first)
  // encode c_last to 10bits
  uint16_t encoded_c_last = 0;
  {
    uint16_t len_cnt = 0;
    const char *const c_last = k.c_last.data();
    for (int syl = 0; syl < 3; ++syl) {
      uint16_t code = 0;
      uint16_t len = 0;
      switch (c_last[len_cnt]) {
      case 'A': {
        switch (c_last[len_cnt + 1]) {
          case 'B': {code = 0; len = 4; break;}
          case 'N': {code = 1; len = 4; break;}
          case 'T': {code = 2; len = 5; break;}
        }
        break;
      }
      case 'B': {code = 3; len = 3; break;}
      case 'C': {code = 4; len = 5; break;}
      case 'E': {
        switch (c_last[len_cnt + 1]) {
          case 'I': {code = 5; len = 4; break;}
          case 'S': {code = 6; len = 3; break;}
        }
        break;
      }
      case 'O': {code = 7; len = 5; break;}
      case 'P': {
        switch (c_last[len_cnt + 2]) {
          case 'E': {code = 8; len = 4; break;}
          case 'I': {code = 9; len = 3; break;}
        }
        break;
      }
      }
      assert(len > 0);
      encoded_c_last = encoded_c_last * 10 + code;
      len_cnt += len;
    }
  }
  // encode the first 5 alnums of c_fist to 30bits
  uint32_t encoded_c_first = 0;
  if (min_c_first) {
    encoded_c_first = 0;
  }
  else if (max_c_first) {
    encoded_c_first = 0x3FFFFFFFUL;
  }
  else {
    const char *const c_first = k.c_first.data();
    for (int ch = 0; ch < 5; ++ch) {
      uint32_t code;
      if (c_first[ch] <= '9') code = (uint32_t)(c_first[ch] - '0');
      else if (c_first[ch] <= 'Z') code = (uint32_t)(c_first[ch] - ('A' - 10));
      else code = (uint32_t)(c_first[ch] - ('a' - 36));
      encoded_c_first = (encoded_c_first << 6) | code;
    }
  }
  return (((uint64_t)k.c_w_id) << 44) | (((uint64_t)k.c_d_id) << 40) |
    (((uint64_t)encoded_c_last) << 30) | (((uint64_t)encoded_c_first));
}

static inline uint64_t district(const district::key &k) {
  // w_id, 4b d_id
  return (((uint64_t)k.d_w_id) << 4) | (((uint64_t)k.d_id));
}

static inline uint64_t history(const history::key &k) {
  // overlap and xor
  return (((uint64_t)k.h_c_id) << 40) ^ (((uint64_t)k.h_c_d_id) << 32) ^
    (((uint64_t)k.h_c_w_id) << 24) ^ (((uint64_t)k.h_d_id) << 16) ^
    (((uint64_t)k.h_w_id) << 8) ^ (((uint64_t)k.h_date));
}

static inline uint64_t item(const item::key &k) {
  return (uint64_t)k.i_id;
}

static constexpr uint64_t new_order_interval = 0x40000000UL;
static inline uint64_t new_order(const new_order::key &k) {
  // w_id, 4b d_id, 20b o_id
  return (((uint64_t)k.no_w_id) << 24) | (((uint64_t)k.no_d_id) << 20) | (((uint64_t)k.no_o_id));
}

static inline uint64_t oorder(const oorder::key &k) {
  // w_id, 4b d_id, 20b o_id
  return (((uint64_t)k.o_w_id) << 24) | (((uint64_t)k.o_d_id) << 20) | (((uint64_t)k.o_id));
}

static constexpr uint64_t oorder_c_id_idx_interval = 0x40000000UL;
static inline uint64_t oorder_c_id_idx(const oorder_c_id_idx::key &k) {
  // w_id, 4b d_id, 20b c_id, 20b o_id
  // o_id is reversed!
  return (((uint64_t)k.o_w_id) << 44) | (((uint64_t)k.o_d_id) << 40) |
    (((uint64_t)k.o_c_id) << 20) | ((0xFFFFFULL - (uint64_t)k.o_o_id));
}

static constexpr uint64_t order_line_interval = 0x10UL;
static inline uint64_t order_line(const order_line::key &k) {
  // w_id, 4b d_id, 20b o_id, 4b ol_number
  return (((uint64_t)k.ol_w_id) << 28) | (((uint64_t)k.ol_d_id) << 24) |
    (((uint64_t)k.ol_o_id) << 4) | (((uint64_t)k.ol_number));
}

static inline uint64_t stock(const stock::key &k) {
  // w_id, 18b i_id
  return (((uint64_t)k.s_w_id) << 18) | (((uint64_t)k.s_i_id));
}

static inline uint64_t stock_data(const stock_data::key &k) {
  // w_id, 18b i_id
  return (((uint64_t)k.s_w_id) << 18) | (((uint64_t)k.s_i_id));
}

static inline uint64_t warehouse(const warehouse::key &k) {
  return (uint64_t)k.w_id;
}

/*static inline uint64_t nation(const nation::key &k) {
  return (uint64_t)k.n_nationkey;
}

static inline uint64_t region(const region::key &k) {
  return (uint64_t)k.r_regionkey;
}

static inline uint64_t supplier(const supplier::key &k) {
  return (uint64_t)k.su_suppkey;
}*/

}