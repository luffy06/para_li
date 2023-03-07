#ifndef NFL_PARA_H
#define NFL_PARA_H

#include "core/afli_para_impl.h"
#include "core/numerical_flow.h"
#include "core/conflicts.h"
#include "core/common.h"

namespace aflipara {

template<typename KT, typename VT>
class NFLPara {
typedef std::pair<KT, VT> KVT;
typedef std::pair<double, KVT> KKVT;
public:
  uint32_t num_bg;
  // A global buffer 
  uint32_t max_buffer_size;
  uint32_t buffer_size;
  KVT* buffer;
  KVT* imm_buffer;
  // Thread pool
  volatile uint8_t buffer_lock;
  volatile uint8_t imm_buffer_lock;
  boost::asio::thread_pool* pool;

  // The learned index
  AFLIPara<KT, VT>* index;

  bool enable_flow;
  NumericalFlow<KT, VT>* flow;
  AFLIPara<double, KVT>* tran_index;

  const float kConflictsDecay = 0.1;
  const uint32_t kMaxBatchSize = 4196;
  const float kSizeAmplification = 1.5;
  const float kTailPercent = 0.99;
public:
  NFLPara(std::string weight_path, uint32_t mbs, uint32_t nb=1);
  ~NFLPara();

  bool buffer_locked();
  void lock_buffer();
  void unlock_buffer();

  bool imm_buffer_locked();
  void lock_imm_buffer();
  void unlock_imm_buffer();

  inline void set_max_buffer_size(uint32_t max_buffer_size);
  void bulk_load(const KVT* kvs, uint32_t size, bool enable_flow=true);
  bool find(KT key, VT& value);
  bool remove(KT key);
  bool update(KVT kv);
  void insert(KVT kv);
  uint64_t model_size();
  uint64_t index_size();

  static void bg_insert(void* args);
};

}

#endif