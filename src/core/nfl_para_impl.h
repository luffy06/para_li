#ifndef NFL_PARA_IMPL_H
#define NFL_PARA_IMPL_H

#include "core/nfl_para.h"

namespace aflipara {

template<typename KT, typename VT>
NFLPara<KT, VT>::NFLPara(std::string weight_path, uint32_t mbs, uint32_t nb) 
                         : max_buffer_size(mbs), num_bg(nb) { 
  this->enable_flow = weight_path != "";
  if (weight_path != "") {
    this->flow = new NumericalFlow<KT, VT>(weight_path, kMaxBatchSize);
  } else {
    this->flow = nullptr;
  }
  this->index = nullptr;
  this->tran_index = nullptr;
  this->buffer_size = 0;
  this->buffer = new KVT[mbs];
  this->imm_buffer = nullptr;
  this->buffer_lock = 0;
  this->imm_buffer_lock = 0;
  if (nb > 0) {
    this->pool = new boost::asio::thread_pool(nb);
  } else {
    this->pool = nullptr;
  }
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
  if (buffer != nullptr) {
    delete[] buffer;
  }
  if (imm_buffer != nullptr) {
    delete[] imm_buffer;
  }
  if (pool != nullptr) {
    delete pool;
  }
}

template<typename KT, typename VT>
bool NFLPara<KT, VT>::buffer_locked() {
  return this->buffer_lock;
}

template<typename KT, typename VT>
void NFLPara<KT, VT>::lock_buffer() {
  uint8_t unlocked = 0, locked = 1;
  while (unlikely(cmpxchgb((uint8_t *)&this->buffer_lock, unlocked, locked) !=
                  unlocked))
    ;
}

template<typename KT, typename VT>
void NFLPara<KT, VT>::unlock_buffer() {
  buffer_lock = 0;
}

template<typename KT, typename VT>
bool NFLPara<KT, VT>::imm_buffer_locked() {
  return this->imm_buffer_lock;
}

template<typename KT, typename VT>
void NFLPara<KT, VT>::lock_imm_buffer() {
  uint8_t unlocked = 0, locked = 1;
  while (unlikely(cmpxchgb((uint8_t *)&this->imm_buffer_lock, unlocked, locked) !=
                  unlocked))
    ;
}

template<typename KT, typename VT>
void NFLPara<KT, VT>::unlock_imm_buffer() {
  imm_buffer_lock = 0;
}

template<typename KT, typename VT>
void NFLPara<KT, VT>::set_max_buffer_size(uint32_t mbs) {
  this->max_buffer_size = mbs;
  this->buffer.reserve(mbs);
}

template<typename KT, typename VT>
void NFLPara<KT, VT>::bulk_load(const KVT* kvs, uint32_t size, 
                                bool enable_flow) {
  if (!enable_flow) {
    index = new AFLIPara<KT, VT>(num_bg, pool);
    index->bulk_load(kvs, size);
  } else {
    KKVT* tran_kvs = new KKVT[size];
    uint32_t origin_tail_conflicts = compute_tail_conflicts(kvs, size, 
                                                            kSizeAmplification, 
                                                            kTailPercent);
    flow->set_batch_size(32);
    flow->transform(kvs, size, tran_kvs);
    std::sort(tran_kvs, tran_kvs + size, [](auto const& a, auto const& b) {
      return a.first < b.first;
    });
    uint32_t tran_tail_conflicts = compute_tail_conflicts(tran_kvs, size, 
                                                          kSizeAmplification, 
                                                          kTailPercent);
    COUT_INFO("Origin Tail Conflicts\t" << origin_tail_conflicts
              << "\nTransformed Tail Conflicts\t" << tran_tail_conflicts)
    exit(-1);
    if (static_cast<int64_t>(origin_tail_conflicts) - tran_tail_conflicts 
        < static_cast<int64_t>(origin_tail_conflicts * kConflictsDecay)) {
      enable_flow = false;
      index = new AFLIPara<KT, VT>(num_bg, pool);
      index->bulk_load(kvs, size);
    } else {
      tran_index = new AFLIPara<double, KVT>(num_bg, pool);
      tran_index->bulk_load(tran_kvs, size);
      flow->set_batch_size(buffer_size);
    }
    delete[] tran_kvs;
  }
  if (enable_flow) {
    COUT_INFO("Enable Flow");
  } else {
    COUT_INFO("Disable Flow");
  }
}

template<typename KT, typename VT>
bool NFLPara<KT, VT>::find(KT key, VT& value) {
  bool in_buffer = false;
  lock_buffer();
  for (uint32_t i = 0; i < buffer_size; ++ i) {
    if (equal(key, buffer[i].first)) {
      value = buffer[i].second;
      in_buffer = true;
    }
  }
  unlock_buffer();
  if (in_buffer) {
    COUT_INFO("Find in buffer");
    return true;
  } else {
    COUT_INFO("Start to find in index");
    if (enable_flow) {
      KKVT tran_kv = flow->transform({key, 0});
      KVT kv;
      bool res = tran_index->find(tran_kv.first, kv);
      value = kv.second;
      return res;
    } else {
      return index->find(key, value);
    }
  }
}

template<typename KT, typename VT>
bool NFLPara<KT, VT>::remove(KT key) {
  bool in_buffer = false;
  lock_buffer();
  for (uint32_t i = 0; i < buffer_size; ++ i) {
    if (equal(key, buffer[i].first)) {
      in_buffer = true;
    }
    if (in_buffer) {
      if (i < buffer_size - 1) {
        buffer[i] = buffer[i + 1];
      } else {
        buffer_size --;
      }
    }
  }
  unlock_buffer();
  if (in_buffer) {
    return true;
  } else {
    if (enable_flow) {
      KKVT tran_kv = flow->transform({key, 0});
      return tran_index->remove(tran_kv.first);
    } else {
      return index->remove(key);
    }
  }
}

template<typename KT, typename VT>
bool NFLPara<KT, VT>::update(KVT kv) {
  bool in_buffer = false;
  lock_buffer();
  for (uint32_t i = 0; i < buffer_size; ++ i) {
    if (equal(kv.first, buffer[i].first)) {
      buffer[i] = kv;
      in_buffer = true;
    }
  }
  unlock_buffer();
  if (in_buffer) {
    return true;
  } else {
    if (enable_flow) {
      KKVT tran_kv = flow->transform(kv);
      return tran_index->update(tran_kv);
    } else {
      return index->update(kv);
    }
  } 
}

template<typename KT, typename VT>
void NFLPara<KT, VT>::insert(KVT kv) {
  lock_buffer();
  buffer[buffer_size] = kv;
  buffer_size ++;
  if (buffer_size == max_buffer_size) {
    while (imm_buffer_locked()) { }
    lock_imm_buffer();
    imm_buffer = buffer;
    buffer = new KVT[max_buffer_size];
    buffer_size = 0;
    if (pool != nullptr) {
      boost::asio::post(*pool, boost::bind(NFLPara<KT, VT>::bg_insert, this));
    } else {
      NFLPara<KT, VT>::bg_insert(this);
    }
  }
  unlock_buffer();
}

template<typename KT, typename VT>
void NFLPara<KT, VT>::bg_insert(void* args) {
  NFLPara<KT, VT>* nfl = (NFLPara<KT, VT>*)(args);
  KVT* kvs = nfl->imm_buffer;
  uint32_t size = nfl->max_buffer_size;
  if (nfl->enable_flow) {
    KKVT* tran_kvs = new KKVT[size];
    nfl->flow->transform(kvs, size, tran_kvs);
    for (uint32_t i = 0; i < size; ++ i) {
      nfl->tran_index->insert(tran_kvs[i]);
    }
    delete[] tran_kvs;
  } else {
    for (uint32_t i = 0; i < size; ++ i) {
      nfl->index->insert(kvs[i]);
    }
  }
  delete[] nfl->imm_buffer;
  nfl->imm_buffer = nullptr;
  nfl->unlock_imm_buffer();
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
          + sizeof(NFLPara<KT, VT>);
  } else {
    return index->index_size() + sizeof(NFLPara<KT, VT>);
  }
}

}

#endif