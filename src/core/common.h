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
  while (unlikely(cmpxchgb((uint8_t*)&out_locked, unlocked, locked) !=
                  unlocked))
    ;
}

void unlock_stdout() {
  out_locked = 0;
}

#define COUT_INFO_W_LOCK(this) \
  lock_stdout(); \
  std::cout << std::fixed << this << std::endl; \
  unlock_stdout();
#define UNUSED(var) ((void)var)

const long long kSeed = 1e9 + 7;
const int kINF = 0x7fffffff;


// Help functions
template<typename T>
inline bool equal(const T& a, const T& b) {
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
      ASSERT_WITH_MSG(false, ss + " is not a number");
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
  std::mt19937_64 gen(kSeed);
  for (int i = l; i < r; ++ i) {
    long long rv = gen();
    int j = std::abs(rv) % (r - i) + i;
    std::swap(kvs[j], kvs[i]);
  }
}

}

#endif