#ifndef AFLI_NODE_PARA_IMPL_H
#define AFLI_NODE_PARA_IMPL_H

#include "core/afli_node_para.h"

namespace aflipara {

template<typename KT, typename VT>
TNodePara<KT, VT>::TNodePara(uint32_t id) {
  this->id = id;
  this->model = nullptr;
  this->capacity = 0;
  this->bitmap0 = this->bitmap1 = nullptr;
  this->entries = nullptr;
  this->entry_lock = nullptr;
  this->node_lock = 0;
}

template<typename KT, typename VT>
TNodePara<KT, VT>::~TNodePara() {
  destroy_self();
}

template<typename KT, typename VT>
uint32_t TNodePara<KT, VT>::get_capacity() { return capacity; }

// User API interfaces
template<typename KT, typename VT>
bool TNodePara<KT, VT>::find(KT key, VT& value, uint32_t depth) {
  // Find the key-value pair in the model node.
  uint32_t idx = std::min(std::max(model->predict(key), 0L), 
                          static_cast<int64_t>(capacity - 1));
  lock_entry(idx);
  uint8_t type = entry_type(idx);
  if (type == kData) {
    KVT kv = entries[idx].kv;
    unlock_entry(idx);
    if (equal(kv.first, key)) {
      value = kv.second;
      return true;
    } else {
      return false;
    }
  } else if (type == kBucket) {
    Bucket<KT, VT>* bucket = entries[idx].bucket;
    ASSERT_WITH_MSG(bucket != nullptr, "Null bucket");
    bool res = bucket->find(key, value);
    unlock_entry(idx);
    return res;
  } else {
    TNodePara<KT, VT>* child = entries[idx].child;
    ASSERT_WITH_MSG(child != nullptr, "Null child node");
    unlock_entry(idx);
    return child->find(key, value, depth + 1);
  }
}

template<typename KT, typename VT>
bool TNodePara<KT, VT>::update(KVT kv) {
  // Update the key-value pair in the model node.
  uint32_t idx = std::min(std::max(model->predict(kv.first), 0L), 
                          static_cast<int64_t>(capacity - 1));
  lock_entry(idx);
  uint8_t type = entry_type(idx);
  if (type == kData) {
    bool res = false;
    if (equal(entries[idx].kv.first, kv.first)) {
      entries[idx].kv = kv;
      res = true;
    } else {
      res = false;
    }
    unlock_entry(idx);
    return res;
  } else if (type == kBucket) {
    Bucket<KT, VT>* bucket = entries[idx].bucket;
    bool res = bucket->update(kv);
    unlock_entry(idx);
    return res;
  } else {
    TNodePara<KT, VT>* child = entries[idx].child;
    unlock_entry(idx);
    return child->update(kv);
  }
}

template<typename KT, typename VT>
bool TNodePara<KT, VT>::remove(KT key) {
  // Remove the key-value pair in the model node.
  uint32_t idx = std::min(std::max(model->predict(key), 0L), 
                          static_cast<int64_t>(capacity - 1));
  lock_entry(idx);
  uint8_t type = entry_type(idx);
  if (type == kData) {
    bool res = false;
    KVT kv = entries[idx].kv;
    if (equal(kv.first, key)) {
      set_entry_type(idx, kNone);
      res = true;
    } else {
      res = false;
    }
    unlock_entry(idx);
    return res;
  } else if (type == kBucket) {
    Bucket<KT, VT>* bucket = entries[idx].bucket;
    bool res = bucket->remove(key);
    unlock_entry(idx);
    return res;
  } else {
    TNodePara<KT, VT>* child = entries[idx].child;
    unlock_entry(idx);
    return child->remove(key);;
  }
}

template<typename KT, typename VT>
RebuildInfo<KT, VT>* TNodePara<KT, VT>::insert(KVT kv, uint32_t depth, 
                                               HyperParameter& hyper_para) {
  uint32_t idx = std::min(std::max(model->predict(kv.first), 0L), 
                          static_cast<int64_t>(capacity - 1));
  lock_entry(idx);
  uint8_t type = entry_type(idx);
  if (type == kNone) {
    set_entry_type(idx, kData);
    entries[idx].kv = kv;
    unlock_entry(idx);
    return nullptr;
  } else if (type == kData || type == kBucket) {
    Bucket<KT, VT>* bucket = nullptr;
    if (type == kData) {
      KVT stored_kv = entries[idx].kv;
      bucket = new Bucket<KT, VT>(&stored_kv, 1, hyper_para.max_bucket_size, 
                                  id, idx);
      entries[idx].bucket = bucket;
      set_entry_type(idx, kBucket);
    }
    bucket = entries[idx].bucket;
    bool need_rebuild = bucket->insert(kv, hyper_para.max_bucket_size);
    if (need_rebuild) {
      return new RebuildInfo(this, depth, idx, hyper_para);
    } else {
      unlock_entry(idx);
      return nullptr;
    }
  } else {
    TNodePara<KT, VT>* child = entries[idx].child;
    unlock_entry(idx);
    return child->insert(kv, depth + 1, hyper_para);
  }
}

template<typename KT, typename VT>
bool TNodePara<KT, VT>::node_locked() {
  return this->node_lock;
}

template<typename KT, typename VT>
void TNodePara<KT, VT>::lock_node() {
  uint8_t unlocked = 0, locked = 1;
  while (unlikely(cmpxchgb((uint8_t *)&this->node_lock, unlocked, locked) !=
                  unlocked))
    ;
}

template<typename KT, typename VT>
void TNodePara<KT, VT>::unlock_node() {
  node_lock = 0;
}

template<typename KT, typename VT>
bool TNodePara<KT, VT>::entry_locked(uint32_t idx) {
  return this->entry_lock[idx];
}

template<typename KT, typename VT>
void TNodePara<KT, VT>::lock_entry(uint32_t idx) {
  uint8_t unlocked = 0, locked = 1;
  while (unlikely(cmpxchgb((uint8_t *)&this->entry_lock[idx], unlocked, locked) 
                  != unlocked)) { }
}

template<typename KT, typename VT>
void TNodePara<KT, VT>::unlock_entry(uint32_t idx) {
  this->entry_lock[idx] = 0;
}

template<typename KT, typename VT>
uint8_t TNodePara<KT, VT>::entry_type(uint32_t idx) {
  uint32_t bit_idx = BIT_IDX(idx);
  uint32_t bit_pos = BIT_POS(idx);
  uint8_t bit0 = GET_BIT(bitmap0[bit_idx], bit_pos);
  uint8_t bit1 = GET_BIT(bitmap1[bit_idx], bit_pos);
  return (bit0 | (bit1 << 1));
}

template<typename KT, typename VT>
void TNodePara<KT, VT>::set_entry_type(uint32_t idx, uint8_t type) {
  uint32_t bit_idx = BIT_IDX(idx);
  uint32_t bit_pos = BIT_POS(idx);
  if (GET_BIT(bitmap0[bit_idx], bit_pos) ^ GET_BIT(type, 0)) {
    REV_BIT(bitmap0[bit_idx], bit_pos);
  }
  if (GET_BIT(bitmap1[bit_idx], bit_pos) ^ GET_BIT(type, 1)) {
    REV_BIT(bitmap1[bit_idx], bit_pos);
  }
}

template<typename KT, typename VT>
void TNodePara<KT, VT>::destroy_self() {    
  delete model;
  model = nullptr;
  for (uint32_t i = 0; i < capacity; ++ i) {
    uint8_t type_i = entry_type(i);
    if (type_i == kBucket) {
      Bucket<KT, VT>* bucket = entries[i].bucket;
      delete bucket;
    } else if (type_i == kNode) {
      TNodePara<KT, VT>* child = entries[i].child;
      uint32_t j = i;
      for (; j < capacity; ++ j) {
        uint8_t type_j = entry_type(j);
        TNodePara<KT, VT>* sibling = entries[j].child;
        if (type_j != kNode || 
            child != sibling) {
          break;
        }
      }
      delete child;
      i = j - 1;
    }
  }
  delete[] bitmap0;
  bitmap0 = nullptr;
  delete[] bitmap1;
  bitmap1 = nullptr;
  delete[] entries;
  entries = nullptr;
  delete[] entry_lock;
  entry_lock = nullptr;
  capacity = 0;
  node_lock = 0;
}

template<typename KT, typename VT>
void TNodePara<KT, VT>::build(const KVT* kvs, uint32_t size, uint32_t depth, 
                              HyperParameter& hyper_para) {
  ConflictsInfo* ci = build_linear_model(kvs, size, model, 
                                         hyper_para.kSizeAmplification);
  // Allocate memory for the node
  uint32_t bit_len = BIT_LEN(ci->max_size);
  capacity = ci->max_size;
  bitmap0 = new BIT_TYPE[bit_len];
  bitmap1 = new BIT_TYPE[bit_len];
  entries = new Entry<KT, VT>[ci->max_size];
  entry_lock = new uint8_t[ci->max_size]; // TODO: optimize space efficiency
  memset(bitmap0, 0, sizeof(BIT_TYPE) * bit_len);
  memset(bitmap1, 0, sizeof(BIT_TYPE) * bit_len);
  for (uint32_t i = 0; i < ci->max_size; ++ i) {
    entry_lock[i] = 0;
  }
  node_lock = 0;
  // Recursively build the node
  for (uint32_t i = 0, j = 0; i < ci->num_conflicts; ++ i) {
    uint32_t p = ci->positions[i];
    uint32_t c = ci->conflicts[i];
    if (c == 0) {
      continue;
    } else if (c == 1) {
      set_entry_type(p, kData);
      entries[p].kv = kvs[j];
      j = j + c;
    } else if (c <= hyper_para.max_bucket_size) {
      set_entry_type(p, kBucket);
      entries[p].bucket = new Bucket<KT, VT>(kvs + j, c, 
                                             hyper_para.max_bucket_size, id, p);
      j = j + c;
    } else {
      uint32_t k = i + 1;
      uint32_t seg_size = c;
      uint32_t end = hyper_para.aggregate_size == 0 ? ci->num_conflicts : 
                      std::min(k + hyper_para.aggregate_size, 
                               ci->num_conflicts);
      while (k < end && ci->positions[k] - ci->positions[k - 1] == 1 && 
             ci->conflicts[k] > hyper_para.max_bucket_size + 1) {
        seg_size += ci->conflicts[k];
        k ++;
      }
      if (seg_size == size) {
        // All conflicted positions are aggregated in one child node 
        // So we build a node for each conflicted position
        for (uint32_t u = i; u < k; ++ u) {
          uint32_t p_k = ci->positions[u];
          uint32_t c_k = ci->conflicts[u];
          set_entry_type(p_k, kNode);
          entries[p_k].child = new TNodePara<KT, VT>(hyper_para.num_nodes ++);
          entries[p_k].child->build(kvs + j, c_k, depth + 1, hyper_para);
          j = j + c_k;
        }
      } else {
        set_entry_type(p, kNode);
        entries[p].child = new TNodePara<KT, VT>(hyper_para.num_nodes ++);
        entries[p].child->build(kvs + j, seg_size, depth + 1, hyper_para);
        for (uint32_t u = i; u < k; ++ u) {
          uint32_t p_k = ci->positions[u];
          set_entry_type(p_k, kNode);
          entries[p_k].child = entries[p].child;
        }
        j = j + seg_size;
      }
      i = k - 1;
    }
  }
  delete ci;
}

}
#endif