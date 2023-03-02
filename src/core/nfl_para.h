#ifndef NFL_PARA_H
#define NFL_PARA_H

#include "core/afli_para.h"
#include "core/numerical_flow.h"
#include "core/common.h"
#include "core/conflicts.h"

namespace aflipara {

template<typename KT, typename VT>
class NFLPara {
typedef std::pair<KT, VT> KVT;
typedef std::pair<KT, KVT> KKVT;
private:
  AFLIPara<KT, VT>* index;
  uint32_t batch_size;
  KVT* batch_kvs;

  bool enable_flow;
  NumericalFlow<KT, VT>* flow;
  AFLIPara<KT, KVT>* tran_index;
  KKVT* tran_kvs;
  uint32_t num_bg;

  const float kConflictsDecay = 0.1;
  const uint32_t kMaxBatchSize = 4196;
  const float kSizeAmplification = 1.5;
  const float kTailPercent = 0.99;
public:
  NFLPara(std::string weights_path, uint32_t bs, uint32_t nb=1);
  ~NFLPara();

  inline void set_batch_size(uint32_t batch_size);
  uint32_t auto_switch(const KVT* kvs, uint32_t size, 
                       uint32_t aggregate_size=0);
  void bulk_load(const KVT* kvs, uint32_t size, uint32_t tail_conflicts, 
                 uint32_t aggregate_size=0);
  void transform(const KVT* kvs, uint32_t size);
  bool find(uint32_t idx_in_batch, VT& value);
  bool update(uint32_t idx_in_batch);
  bool remove(uint32_t idx_in_batch);
  void insert(uint32_t idx_in_batch);
  uint64_t model_size();
  uint64_t index_size();
};

}

#endif