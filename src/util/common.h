#ifndef COMMON_PARA_H
#define COMMON_PARA_H

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/optional.hpp>
#include <boost/program_options.hpp>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <getopt.h>
#include <immintrin.h>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <queue>
#include <random>
#include <set>
#include <sstream>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <thread>
#include <vector>
#include <unistd.h>

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

namespace po = boost::program_options;

namespace aflipara {

#define BIT_TYPE uint8_t
#define BIT_SIZE (sizeof(BIT_TYPE) * 8)
#define BIT_LEN(x) (std::ceil((x) * 1. / BIT_SIZE))
#define BIT_IDX(x) ((x) / BIT_SIZE)
#define BIT_POS(x) ((x) % BIT_SIZE)
#define SET_BIT_ONE(x, n) ((x) |= (1 << (n)))
#define SET_BIT_ZERO(x, n) ((x) &= (~(1 << (n))))
#define REV_BIT(x, n) ((x) ^= (1 << (n)))
#define GET_BIT(x, n) (((x) >> (n)) & 1)

#define COUT_INFO(this) std::cout << std::fixed << this << std::endl;
#define COUT_ERR(this) \
  std::cerr << std::fixed << this << std::endl; \
  assert(false);

#define UNUSED(var) ((void)var)
#define ASSERT_WITH_MSG(cond, msg) \
  { \
    if (!(cond)) { \
      COUT_ERR("Assertion at " << __FILE__ << ":" << __LINE__ << ", error: " << msg) \
    } \
  }

#define TIME_LOG (std::chrono::high_resolution_clock::now())
#define TIME_IN_NANO_SECOND(begin, end) (std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count())
#define TIME_IN_SECOND(begin, end) (TIME_IN_NANO_SECOND(begin, end) / 1e9)

inline void memory_fence() { asm volatile("mfence" : : : "memory"); }

inline void fence() { asm volatile("" : : : "memory"); }

inline uint64_t cmpxchg(uint64_t *object, uint64_t expected,
                               uint64_t desired) {
  asm volatile("lock; cmpxchgq %2,%1"
               : "+a"(expected), "+m"(*object)
               : "r"(desired)
               : "cc");
  fence();
  return expected;
}

inline uint8_t cmpxchgb(uint8_t *object, uint8_t expected,
                               uint8_t desired) {
  asm volatile("lock; cmpxchgb %2,%1"
               : "+a"(expected), "+m"(*object)
               : "r"(desired)
               : "cc");
  fence();
  return expected;
}

volatile uint8_t out_locked = 0;

void lock_stdout() {
  uint8_t unlocked = 0, locked = 1;
  while (unlikely(cmpxchgb((uint8_t *)&out_locked, unlocked, locked) !=
                  unlocked))
    ;
}

void unlock_stdout() {
  out_locked = 0;
}

#define COUT_W_LOCK(this) \
  lock_stdout(); \
  std::cout << std::fixed << this << std::endl; \
  unlock_stdout();
#define UNUSED(var) ((void)var)

const long long kSEED = 1e9 + 7;
const int kINF = 0x7fffffff;

enum OperationType {
  kBulkLoad = 0,
  kQuery = 1,
  kInsert = 2,
  kUpdate = 3,
  kDelete = 4
};

template<typename KT, typename VT>
struct Request {
  OperationType op;
  std::pair<KT, VT> kv;
};

inline void assert_p(bool condition, const std::string& error_msg) {
  if (!condition) {
    std::cerr << error_msg << std::endl;
    exit(-1);
  }
}

template<typename T>
inline bool compare(const T& a, const T& b) {
  if (std::numeric_limits<T>::is_integer) {
    return a == b;
  } else {
    return std::fabs(a - b) < std::numeric_limits<T>::epsilon();
  }
}

template<typename T>
std::string tostr(T n) {
  std::stringstream ss;
  ss << std::setprecision(std::numeric_limits<T>::digits10) << std::fixed << n;
  std::string n_str = ss.str();
  if (n_str.find(".") != std::string::npos) {
    while (*(n_str.rbegin()) == '0') {
      n_str.pop_back();
    }
    if (*(n_str.rbegin()) == '.') {
      n_str.pop_back();
    }
  }
  return n_str;
}

template<typename T, typename P>
P ston(T s) {
  std::string ss = tostr<T>(s);
  P v = 0;
  int point = -1;
  bool negative = (ss[0] == '-');
  for (int i = (negative ? 1 : 0); i < ss.size(); ++ i) {
    if (ss[i] >= '0' && ss[i] <= '9') {
      v = v * 10 + (ss[i] - '0');
    } else if (point == -1 && ss[i] == '.') {
      point = ss.size() - i - 1;
    } else {
      assert_p(false, ss + " is not a number");
    }
  }
  for (int i = 0; i < point; ++ i) {
    v = v / 10.;
  }
  if (negative) {
    v = -v;
  }
  return v;
}

bool start_with(std::string src, std::string target) {
  if (src.size() >= target.size()) {
    for (int i = 0; i < target.size(); ++ i) {
      if (src[i] != target[i]) {
        return false;
      }
    }
    return true;
  }
  return false;
}

template<typename T>
void shuffle(std::vector<T>& kvs, int l, int r) {
  std::mt19937_64 gen(kSEED);
  for (int i = l; i < r; ++ i) {
    long long rv = gen();
    int j = std::abs(rv) % (r - i) + i;
    std::swap(kvs[j], kvs[i]);
  }
}

void check_options(const po::variables_map& vm, 
                   const std::vector<std::string>& options) {
  for (auto op : options) {
    if (!vm.count(op)) {
      std::cout << "--" << op << " option required" << std::endl;
    }
  }
}

std::string GetWorkloadName(std::string workload_path) {
  int l = 0;
  int r = workload_path.size();
  for (int i = r; i >= 0; -- i) {
    if (workload_path[i] == '.') {
      r = i;
      break;
    }
  }
  for (int i = 0; i < r; ++ i) {
    if (workload_path[i] == '/') {
      l = i + 1;
    }
  }
  return workload_path.substr(l, r - l);
}

std::string path_join(std::string patha, std::string pathb) {
  int idxa = patha.size() - 1;
  while (idxa > 0 && patha[idxa] == '/') {
    idxa --;
  }
  int idxb = 0;
  while (idxb < pathb.size() && pathb[idxb] == '/') {
    idxb ++;
  }
  if (idxa == -1 && idxb == pathb.size()) {
    return "/";
  } else if (idxa == -1) {
    return "/" + pathb.substr(idxb, pathb.size() - idxb);
  } else if (idxb == pathb.size()) {
    return patha.substr(0, idxa + 1);
  } else {
    return patha.substr(0, idxa + 1) + "/" 
            + pathb.substr(idxb, pathb.size() - idxb);
  }
}

template <class val_t>
struct AtomicVal {
  union ValUnion;
  typedef ValUnion val_union_t;
  typedef val_t value_type;
  union ValUnion {
    val_t val;
    AtomicVal *ptr;
    ValUnion() {}
    ValUnion(val_t val) : val(val) {}
    ValUnion(AtomicVal *ptr) : ptr(ptr) {}
  };

  // 60 bits for version
  static const uint64_t version_mask = 0x0fffffffffffffff;
  static const uint64_t lock_mask = 0x1000000000000000;
  static const uint64_t removed_mask = 0x2000000000000000;
  static const uint64_t pointer_mask = 0x4000000000000000;

  val_union_t val;
  // lock - removed - is_ptr
  volatile uint64_t status;

  AtomicVal() : status(0) {}
  AtomicVal(val_t val) : val(val), status(0) {}
  AtomicVal(AtomicVal *ptr) : val(ptr), status(0) { set_is_ptr(); }

  bool is_ptr(uint64_t status) { return status & pointer_mask; }
  bool removed(uint64_t status) { return status & removed_mask; }
  bool is_ptr() { return status & pointer_mask; }
  bool removed() {
    if (is_ptr()) return this->val.ptr->removed();
    return status & removed_mask;
  }
  bool locked(uint64_t status) { return status & lock_mask; }
  uint64_t get_version(uint64_t status) { return status & version_mask; }

  void set_is_ptr() { status |= pointer_mask; }
  void unset_is_ptr() { status &= ~pointer_mask; }
  void set_removed() { status |= removed_mask; }
  void lock() {
    while (true) {
      uint64_t old = status;
      uint64_t expected = old & ~lock_mask;  // expect to be unlocked
      uint64_t desired = old | lock_mask;    // desire to lock
      if (likely(cmpxchg((uint64_t *)&this->status, expected, desired) ==
                 expected)) {
        return;
      }
    }
  }
  void unlock() { status &= ~lock_mask; }
  void incr_version() {
    uint64_t version = get_version(status);
    UNUSED(version);
    status++;
    assert(get_version(status) == version + 1);
  }

  // semantics: atomically read the value and the `removed` flag
  bool read(val_t &val) {
    while (true) {
      uint64_t status = this->status;
      memory_fence();
      val_union_t val_union = this->val;
      memory_fence();

      uint64_t current_status = this->status;
      memory_fence();

      if (unlikely(locked(current_status))) {  // check lock
        continue;
      }

      if (likely(get_version(status) ==
                 get_version(current_status))) {  // check version
        if (unlikely(is_ptr(status))) {
          assert(!removed(status));
          return val_union.ptr->read(val);
        } else {
          val = val_union.val;
          return !removed(status);
        }
      }
    }
  }
  bool read_without_lock(val_t &val) {
    while (true) {
      uint64_t status = this->status;
      memory_fence();
      val_union_t val_union = this->val;
      memory_fence();

      uint64_t current_status = this->status;
      memory_fence();

      if (likely(get_version(status) ==
                 get_version(current_status))) {  // check version
        if (unlikely(is_ptr(status))) {
          assert(!removed(status));
          return val_union.ptr->read(val);
        } else {
          val = val_union.val;
          return !removed(status);
        }
      }
    }
  }
  bool update_without_lock(const val_t &val) {
    uint64_t status = this->status;
    bool res;
    if (unlikely(is_ptr(status))) {
      assert(!removed(status));
      res = this->val.ptr->update(val);
    } else if (!removed(status)) {
      this->val.val = val;
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    return res;
  }
  bool update(const val_t &val) {
    lock();
    uint64_t status = this->status;
    bool res;
    if (unlikely(is_ptr(status))) {
      assert(!removed(status));
      res = this->val.ptr->update(val);
    } else if (!removed(status)) {
      this->val.val = val;
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
    return res;
  }
  bool remove() {
    lock();
    uint64_t status = this->status;
    bool res;
    if (unlikely(is_ptr(status))) {
      assert(!removed(status));
      res = this->val.ptr->remove();
    } else if (!removed(status)) {
      set_removed();
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
    return res;
  }
  void replace_pointer() {
    lock();
    uint64_t status = this->status;
    UNUSED(status);
    assert(is_ptr(status));
    assert(!removed(status));
    if (!val.ptr->read(val.val)) {
      set_removed();
    }
    unset_is_ptr();
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
  }
  bool read_ignoring_ptr(val_t &val) {
    while (true) {
      uint64_t status = this->status;
      memory_fence();
      val_union_t val_union = this->val;
      memory_fence();
      if (unlikely(locked(status))) {
        continue;
      }
      memory_fence();

      uint64_t current_status = this->status;
      if (likely(get_version(status) == get_version(current_status))) {
        val = val_union.val;
        return !removed(status);
      }
    }
  }
  bool update_ignoring_ptr(const val_t &val) {
    lock();
    uint64_t status = this->status;
    bool res;
    if (!removed(status)) {
      this->val.val = val;
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
    return res;
  }
  bool remove_ignoring_ptr() {
    lock();
    uint64_t status = this->status;
    bool res;
    if (!removed(status)) {
      set_removed();
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
    return res;
  }
};

// template <class val_t>
// struct AtomicVal {
//   val_t val;
//   volatile uint8_t status;

//   AtomicVal() : status(0) {}
//   AtomicVal(val_t val) : val(val), status(0) {}
//   AtomicVal(AtomicVal *ptr) : val(ptr), status(0) { set_is_ptr(); }

//   bool is_ptr(uint64_t status) { return status & pointer_mask; }
//   bool removed(uint64_t status) { return status & removed_mask; }
//   bool is_ptr() { return status & pointer_mask; }
//   bool removed() {
//     if (is_ptr()) return this->val.ptr->removed();
//     return status & removed_mask;
//   }
//   bool locked(uint64_t status) { return status & lock_mask; }
//   uint64_t get_version(uint64_t status) { return status & version_mask; }

//   void set_is_ptr() { status |= pointer_mask; }
//   void unset_is_ptr() { status &= ~pointer_mask; }
//   void set_removed() { status |= removed_mask; }
//   void lock() {
//     while (true) {
//       uint64_t old = status;
//       uint64_t expected = old & ~lock_mask;  // expect to be unlocked
//       uint64_t desired = old | lock_mask;    // desire to lock
//       if (likely(cmpxchg((uint64_t *)&this->status, expected, desired) ==
//                  expected)) {
//         return;
//       }
//     }
//   }
//   void unlock() { status &= ~lock_mask; }
//   void incr_version() {
//     uint64_t version = get_version(status);
//     UNUSED(version);
//     status++;
//     assert(get_version(status) == version + 1);
//   }

//   // semantics: atomically read the value and the `removed` flag
//   bool read(val_t &val) {
//     while (true) {
//       uint64_t status = this->status;
//       memory_fence();
//       val_union_t val_union = this->val;
//       memory_fence();

//       uint64_t current_status = this->status;
//       memory_fence();

//       if (unlikely(locked(current_status))) {  // check lock
//         continue;
//       }

//       if (likely(get_version(status) ==
//                  get_version(current_status))) {  // check version
//         if (unlikely(is_ptr(status))) {
//           assert(!removed(status));
//           return val_union.ptr->read(val);
//         } else {
//           val = val_union.val;
//           return !removed(status);
//         }
//       }
//     }
//   }
//   bool read_without_lock(val_t &val) {
//     while (true) {
//       uint64_t status = this->status;
//       memory_fence();
//       val_union_t val_union = this->val;
//       memory_fence();

//       uint64_t current_status = this->status;
//       memory_fence();

//       if (likely(get_version(status) ==
//                  get_version(current_status))) {  // check version
//         if (unlikely(is_ptr(status))) {
//           assert(!removed(status));
//           return val_union.ptr->read(val);
//         } else {
//           val = val_union.val;
//           return !removed(status);
//         }
//       }
//     }
//   }
//   bool update_without_lock(const val_t &val) {
//     uint64_t status = this->status;
//     bool res;
//     if (unlikely(is_ptr(status))) {
//       assert(!removed(status));
//       res = this->val.ptr->update(val);
//     } else if (!removed(status)) {
//       this->val.val = val;
//       res = true;
//     } else {
//       res = false;
//     }
//     memory_fence();
//     incr_version();
//     memory_fence();
//     return res;
//   }
//   bool update(const val_t &val) {
//     lock();
//     uint64_t status = this->status;
//     bool res;
//     if (unlikely(is_ptr(status))) {
//       assert(!removed(status));
//       res = this->val.ptr->update(val);
//     } else if (!removed(status)) {
//       this->val.val = val;
//       res = true;
//     } else {
//       res = false;
//     }
//     memory_fence();
//     incr_version();
//     memory_fence();
//     unlock();
//     return res;
//   }
//   bool remove() {
//     lock();
//     uint64_t status = this->status;
//     bool res;
//     if (unlikely(is_ptr(status))) {
//       assert(!removed(status));
//       res = this->val.ptr->remove();
//     } else if (!removed(status)) {
//       set_removed();
//       res = true;
//     } else {
//       res = false;
//     }
//     memory_fence();
//     incr_version();
//     memory_fence();
//     unlock();
//     return res;
//   }
//   void replace_pointer() {
//     lock();
//     uint64_t status = this->status;
//     UNUSED(status);
//     assert(is_ptr(status));
//     assert(!removed(status));
//     if (!val.ptr->read(val.val)) {
//       set_removed();
//     }
//     unset_is_ptr();
//     memory_fence();
//     incr_version();
//     memory_fence();
//     unlock();
//   }
//   bool read_ignoring_ptr(val_t &val) {
//     while (true) {
//       uint64_t status = this->status;
//       memory_fence();
//       val_union_t val_union = this->val;
//       memory_fence();
//       if (unlikely(locked(status))) {
//         continue;
//       }
//       memory_fence();

//       uint64_t current_status = this->status;
//       if (likely(get_version(status) == get_version(current_status))) {
//         val = val_union.val;
//         return !removed(status);
//       }
//     }
//   }
//   bool update_ignoring_ptr(const val_t &val) {
//     lock();
//     uint64_t status = this->status;
//     bool res;
//     if (!removed(status)) {
//       this->val.val = val;
//       res = true;
//     } else {
//       res = false;
//     }
//     memory_fence();
//     incr_version();
//     memory_fence();
//     unlock();
//     return res;
//   }
//   bool remove_ignoring_ptr() {
//     lock();
//     uint64_t status = this->status;
//     bool res;
//     if (!removed(status)) {
//       set_removed();
//       res = true;
//     } else {
//       res = false;
//     }
//     memory_fence();
//     incr_version();
//     memory_fence();
//     unlock();
//     return res;
//   }
// };

struct TreeStat {
  // Configs
  uint32_t bucket_size_ = 0;
  uint32_t max_aggregate_ = 0;
  // Node counts
  uint32_t num_model_nodes_ = 0;
  uint32_t num_buckets_ = 0;
  uint32_t num_dense_nodes_ = 0;
  // Data counts
  uint32_t num_data_model_ = 0;
  uint32_t num_data_bucket_ = 0;
  uint32_t num_data_dense_ = 0;
  // Depth
  uint32_t num_leaf_nodes_ = 0;
  uint32_t sum_depth_ = 0;
  uint32_t max_depth_ = 0;
  // Size
  uint64_t model_size_ = 0;
  uint64_t index_size_ = 0;
  // Conflicts
  double node_conflicts_ = 0;

  uint32_t num_data() {
    return num_data_model_ + num_data_bucket_ + num_data_dense_;
  }

  double avg_depth() {
    return sum_depth_ * 1. / num_data();
  }

  void show() {
    std::cout << std::string(15, '#') << "Tree Statistics" 
              << std::string(15, '#') << std::endl;
    std::cout << "Bucket Threshold\t" << bucket_size_ << std::endl;
    std::cout << "Max Aggregate Size\t" << max_aggregate_ << std::endl;
    std::cout << "Number of Model Nodes\t" << num_model_nodes_ << std::endl;
    std::cout << "Number of Buckets\t" << num_buckets_ << std::endl;
    std::cout << "Number of Dense Nodes\t" << num_dense_nodes_ << std::endl;
    std::cout << "Number of Data\t" << num_data() << std::endl;
    std::cout << "Number of Data in Model Nodes\t" << num_data_model_ 
              << std::endl;
    std::cout << "Number of Data in Buckets\t" << num_data_bucket_ << std::endl;
    std::cout << "Number of Data in Dense Nodes\t" << num_data_dense_ 
              << std::endl;
    std::cout << "Average Depth\t" << avg_depth() << std::endl;
    std::cout << "Maximum Depth\t" << max_depth_ << std::endl;
    std::cout << "Model Size\t" << model_size_ << std::endl;
    std::cout << "Index Size\t" << index_size_ << std::endl;
    std::cout << "Average conflicts per node\t" << node_conflicts_ / (num_model_nodes_ + num_dense_nodes_) << std::endl;
    std::cout << std::string(45, '#') << std::endl;
  }
};

struct RunStat {
  // # num_data
  uint32_t num_data_ = 0;
  // # requests
  uint32_t num_queries_ = 0;
  uint32_t num_inserts_ = 0;
  uint32_t num_updates_ = 0;
  uint32_t num_query_depth_ = 0;
  uint32_t num_query_model_ = 0;
  uint32_t num_query_bucket_ = 0;
  uint32_t num_query_dense_ = 0;
  uint32_t num_update_model_ = 0;
  uint32_t num_update_bucket_ = 0;
  uint32_t num_update_dense_ = 0;
  uint32_t num_insert_model_ = 0;
  uint32_t num_insert_bucket_ = 0;
  uint32_t num_insert_dense_ = 0;
  uint32_t num_predictions_ = 0;
  uint32_t num_comparisons_ = 0;
  // # internal operations
  uint32_t num_rebuilds_ = 0;
  uint32_t num_data_rebuild_ = 0;
  // Internal statistics
  uint32_t bucket_threshold_ = 0;
  // Space statistics
  uint32_t allocated_size_ = 0;
  // Deprecated
  uint32_t num_cont_conflicts_ = 0;
  uint32_t sum_cont_conflicts_ = 0;
  uint32_t sum_len_cont_conflicts_ = 0;
  uint32_t max_len_cont_conflicts_ = 0;

  uint32_t num_requests() {
    return num_queries_ + num_inserts_ + num_updates_;
  }

  void show() {
    std::cout << std::string(10, '#') << "Running Statistics" 
              << std::string(10, '#') << std::endl;
    std::cout << "Maximum Bucket Size\t" << bucket_threshold_ << std::endl;
    std::cout << "Number of Requests\t" << num_requests() << std::endl;
    std::cout << "Number of Queries\t" << num_queries_ 
              << "\tModel [" << num_query_model_ 
              << "] Bucket [" << num_query_bucket_ 
              << "] Dense [" << num_query_dense_ << "]\nAverage Depth\t" 
              << num_query_depth_ * 1. / num_queries_ << std::endl;
    std::cout << "Number of Updates\t" << num_updates_ 
              << "\tModel [" << num_update_model_ 
              << "] Bucket [" << num_update_bucket_ 
              << "] Dense [" << num_update_dense_ << "]" << std::endl;
    std::cout << "Number of Inserts\t" << num_inserts_ 
              << "\tModel [" << num_insert_model_ 
              << "] Bucket [" << num_insert_bucket_ 
              << "] Dense [" << num_insert_dense_ << "]" << std::endl;
    std::cout << "Number of Predictions\t" << num_predictions_ << std::endl;
    std::cout << "Number of Comparisons\t" << num_comparisons_ << std::endl;
    std::cout << "Number of Rebuilding Times\t" << num_rebuilds_ << std::endl;
    std::cout << "Number of Data in Rebuilding\t" << num_data_rebuild_ 
              << std::endl;
    std::cout << std::string(38, '#') << std::endl;
  }
};

struct ExperimentalResults {
  uint32_t batch_size;
  double bulk_load_trans_time = 0;
  double bulk_load_index_time = 0;
  double sum_transform_time = 0;
  double sum_indexing_time = 0;
  uint32_t num_requests = 0;
  uint64_t model_size = 0;
  uint64_t index_size = 0;
  std::vector<std::pair<double, double>> latencies;
  std::vector<bool> need_compute;
  uint32_t step_count = 0;

  const uint32_t kNumIncrementalReqs = 10000000;

  explicit ExperimentalResults(uint32_t b) : batch_size(b) { }

  void step() {
    if (num_requests / kNumIncrementalReqs > step_count) {
      step_count ++;
      need_compute.push_back(true);
    } else {
      need_compute.push_back(false);
    }
  }

  void show_incremental_throughputs() {
    assert_p(latencies.size() == need_compute.size(), "Internal Error");
    double sum_latency = 0;
    uint32_t num_ops = 0;
    for (uint32_t i = 0; i < latencies.size(); ++ i) {
      sum_latency += latencies[i].first + latencies[i].second;
      num_ops += batch_size;
      if (need_compute[i] || i == latencies.size() - 1) {
        std::cout << num_ops << "\t" << num_ops * 1e3 / sum_latency << std::endl;
      }
    }
  }

  void show(bool pretty=false) {
    if (num_requests == 0) {
      sum_indexing_time = 0;
    }
    std::sort(latencies.begin(), latencies.end(), [](auto const& a, auto const& b) {
      return a.first + a.second < b.first + b.second;
    });
    std::vector<double> tail_percent = {0.5, 0.75, 0.99, 0.995, 0.9999, 1};
    double sum_time = sum_transform_time + sum_indexing_time;
    uint32_t num_ops = num_requests;
    if (pretty) {
      std::cout << std::string(10, '#') << "Experimental Results" 
                << std::string(10, '#') << std::endl;
      std::cout << std::fixed << std::setprecision(6) 
                << "BulkLoad Transformation Time\t" << bulk_load_trans_time / 1e9 << " (s)" << std::endl
                << "BulkLoad Time\t" << bulk_load_index_time / 1e9 << " (s)" << std::endl
                << "Model Size\t"<< model_size << " (bytes)" << std::endl 
                << "Index Size\t" << index_size << " (bytes)" << std::endl
                << "Throughput (Overall)\t" << num_ops * 1e3 / sum_time << " (million ops/sec)" << std::endl 
                << "Average Transform Latency\t" << sum_transform_time / num_ops << " (ns)" << std::endl
                << "Average Indexing Latency\t" << sum_indexing_time / num_ops << " (ns)" << std::endl;
      for (uint32_t i = 0; i < tail_percent.size(); ++ i) {
        uint32_t idx = std::max(0, static_cast<int>(latencies.size() * tail_percent[i]) - 1);
        std::pair<double, double> tail_latency = latencies[idx];
        std::cout << "Tail Latency (P" << tail_percent[i] * 100 <<")\t" 
                  << tail_latency.first / batch_size << " (ns)\t" 
                  << tail_latency.second / batch_size << " (ns)" << std::endl;
      }
      std::cout << std::string(40, '#') << std::endl;
    } else {
      std::cout << std::fixed << std::setprecision(6) 
                << bulk_load_trans_time / 1e9 << "\t"
                << bulk_load_index_time / 1e9 << "\t" 
                << model_size << "\t" 
                << index_size << "\t"
                << num_ops * 1e3 / sum_time << std::endl
                << sum_transform_time / num_ops << "\t"
                << sum_indexing_time / num_ops << std::endl;
      for (uint32_t i = 0; i < tail_percent.size(); ++ i) {
        uint32_t idx = std::max(0, static_cast<int>(latencies.size() * tail_percent[i]) - 1);
        std::pair<double, double> tail_latency = latencies[idx];
        std::cout << tail_latency.first / batch_size  << "\t" << tail_latency.second / batch_size << std::endl;
      }
    }
  }
};



}

#endif