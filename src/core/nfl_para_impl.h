#ifndef NFL_PARA_IMPL_H
#define NFL_PARA_IMPL_H

#include "core/nfl_para.h"

namespace aflipara {

template<typename KT, typename VT>
NFLPara<KT, VT>::NFLPara(std::string weights_path, uint32_t bs, uint32_t nb) 
                         : batch_size(bs), num_bg(nb) { 
  this->enable_flow = true;
  this->flow = new NumericalFlow<KT, VT>(weights_path, bs);
  this->index = nullptr;
  this->tran_index = nullptr;
  this->tran_kvs = nullptr;
  this->batch_kvs = nullptr;
}

template<typename KT, typename VT>
NFLPara<KT, VT>::~NFLPara() {
  if (index != nullptr) {
    delete index;
  }
  if (flow != nullptr) {
    delete flow;
  }
  if (tran_index != nullptr) {
    delete tran_index;
  }
  if (tran_kvs != nullptr) {
    delete[] tran_kvs;
  }
  if (batch_kvs != nullptr) {
    delete[] batch_kvs;
  }
}

template<typename KT, typename VT>
void NFLPara<KT, VT>::set_batch_size(uint32_t batch_size) {
  if (batch_size > this->batch_size) {
    if (enable_flow) {
      delete[] tran_kvs;
      tran_kvs = new KKVT[batch_size];
    } else {
      delete[] batch_kvs;
      batch_kvs = new KVT[batch_size];
    }
  }
  this->batch_size = batch_size;
}

template<typename KT, typename VT>
uint32_t NFLPara<KT, VT>::auto_switch(const KVT* kvs, uint32_t size, 
                              uint32_t aggregate_size) {
  tran_kvs = new KKVT[size];
  uint32_t origin_tail_conflicts = compute_tail_conflicts(kvs, size, 
                                                          kSizeAmplification, 
                                                          kTailPercent);
  flow->set_batch_size(kMaxBatchSize);
  flow->transform(kvs, size, tran_kvs);
  std::sort(tran_kvs, tran_kvs + size, [](auto const& a, auto const& b) {
    return a.first < b.first;
  });
  uint32_t tran_tail_conflicts = compute_tail_conflicts(tran_kvs, size, 
                                                        kSizeAmplification, 
                                                        kTailPercent);
  if (origin_tail_conflicts <= tran_tail_conflicts
      || origin_tail_conflicts - tran_tail_conflicts 
      < static_cast<uint32_t>(origin_tail_conflicts * kConflictsDecay)) {
    enable_flow = false;
    delete[] tran_kvs;
    tran_kvs = nullptr;
    return origin_tail_conflicts;
  } else {
    enable_flow = true;
    return tran_tail_conflicts;
  }
}

template<typename KT, typename VT>
void NFLPara<KT, VT>::bulk_load(const KVT* kvs, uint32_t size, 
                                uint32_t tail_conflicts, 
                                uint32_t aggregate_size) {
  if (enable_flow) {
    tran_index = new AFLIPara<KT, KVT>(num_bg);
    tran_index->bulk_load(tran_kvs, size);
    flow->set_batch_size(batch_size);
    delete[] tran_kvs;
    tran_kvs = new KKVT[batch_size];
  } else {
    index = new AFLIPara<KT, VT>(num_bg);
    index->bulk_load(kvs, size);
    delete[] batch_kvs;
    batch_kvs = new KVT[batch_size];      
  }
}

template<typename KT, typename VT>
void NFLPara<KT, VT>::transform(const KVT* kvs, uint32_t size) {
  if (enable_flow) {
    flow->transform(kvs, size, tran_kvs);
  } else {
    std::memcpy(batch_kvs, kvs, sizeof(KVT) * size);
  }
}

template<typename KT, typename VT>
bool NFLPara<KT, VT>::find(uint32_t idx_in_batch, VT& value) {
  if (enable_flow) {
    return tran_index->find(tran_kvs[idx_in_batch].first, value);
  } else {
    return index->find(batch_kvs[idx_in_batch].first, value);
  }
}

template<typename KT, typename VT>
bool NFLPara<KT, VT>::update(uint32_t idx_in_batch) {
  if (enable_flow) {
    return tran_index->update(tran_kvs[idx_in_batch]);
  } else {
    return index->update(batch_kvs[idx_in_batch]);
  }
}

template<typename KT, typename VT>
bool NFLPara<KT, VT>::remove(uint32_t idx_in_batch) {
  if (enable_flow) {
    return tran_index->remove(tran_kvs[idx_in_batch].first);
  } else {
    return index->remove(batch_kvs[idx_in_batch].first);
  }
}

template<typename KT, typename VT>
void NFLPara<KT, VT>::insert(uint32_t idx_in_batch) {
  if (enable_flow) {
    tran_index->insert(tran_kvs[idx_in_batch]);
  } else {
    index->insert(batch_kvs[idx_in_batch]);
  }
}

template<typename KT, typename VT>
uint64_t NFLPara<KT, VT>::model_size() {
  if (enable_flow) {
    return tran_index->model_size() + flow->size();
  } else {
    return index->model_size();
  }
}

template<typename KT, typename VT>
uint64_t NFLPara<KT, VT>::index_size() {
  if (enable_flow) {
    return tran_index->index_size() + flow->size() 
          + sizeof(NFLPara<KT, VT>) + sizeof(KKVT) * batch_size;
  } else {
    return index->index_size() + sizeof(NFLPara<KT, VT>) + sizeof(KVT) * batch_size;
  }
}

}

#endif