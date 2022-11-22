#ifndef AFLI_NODE_PARA_IMPL_H
#define AFLI_NODE_PARA_IMPL_H

#include "afli_para/afli_node_para.h"

#define BIT_TYPE uint8_t
#define BIT_SIZE (sizeof(BIT_TYPE) * 8)
#define BIT_LEN(x) (std::ceil((x) * 1. / BIT_SIZE))
#define BIT_IDX(x) ((x) / BIT_SIZE)
#define BIT_POS(x) ((x) % BIT_SIZE)
#define SET_BIT_ONE(x, n) ((x) |= (1 << (n)))
#define SET_BIT_ZERO(x, n) ((x) &= (~(1 << (n))))
#define REV_BIT(x, n) ((x) ^= (1 << (n)))
#define GET_BIT(x, n) (((x) >> (n)) & 1)

namespace aflipara {

template<typename KT, typename VT>
TNodePara<KT, VT>::TNodePara(uint32_t id) {
  id = id;
  model = nullptr;
  capacity = 0;
  bitmap0 = bitmap1 = nullptr;
  entries = nullptr;
  bitmap_lock = nullptr;
  // COUT_W_LOCK("Build a new node [" << id << "]")
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
  uint8_t type = entry_type(idx);
  COUT_W_LOCK("T0: Search the " << idx << "th slot")
  Entry<KT, VT> entry;
  entries[idx].read(entry);
  if (type == kData) {
    COUT_W_LOCK("T0: Found [" << std::fixed << key << "] in the " << idx << "th slot of " << id << "th node, depth " << depth)
    if (compare(entry.kv.first, key)) {
      value = entry.kv.second;
      return true;
    } else {
      return false;
    }
  } else if (type == kBucket) {
    COUT_W_LOCK("T0: Try to find [" << std::fixed << key << "] in the " << idx << "th bucket of " << id << "th node, depth " << depth << ", address [" << entry.bucket << "]")
    bool res = entry.bucket->find(key, value);
    COUT_W_LOCK("T0: Found [" << std::fixed << key << "] in the " << idx << "th bucket of " << id << "th node, depth " << depth)
    return res;
  } else if (type == kNode) {
    return entry.child->find(key, value, depth + 1);
  } else {
    return false;
  }
}

template<typename KT, typename VT>
bool TNodePara<KT, VT>::update(KVT kv) {
  // Update the key-value pair in the model node.
  uint32_t idx = std::min(std::max(model->predict(kv.first), 0L), 
                          static_cast<int64_t>(capacity - 1));
  uint8_t type = entry_type(idx);
  Entry<KT, VT> entry;
  entries[idx].read(entry);
  if (type == kData && compare(entry.kv.first, kv.first)) {
    entries[idx].update(Entry<KT, VT>(kv));
    return true;
  } else if (type == kBucket) {
    return entry.bucket->update(kv);
  } else if (type == kNode) {
    return entry.child->update(kv);
  } else {
    return false;
  }
}

template<typename KT, typename VT>
bool TNodePara<KT, VT>::remove(KT key) {
  // Remove the key-value pair in the model node.
  uint32_t idx = std::min(std::max(model->predict(key), 0L), 
                          static_cast<int64_t>(capacity - 1));
  uint8_t type = entry_type(idx);
  Entry<KT, VT> entry;
  entries[idx].read(entry);
  if (type == kData && compare(entry.kv.first, key)) {
    set_entry_type(idx, kNone);
    return true;
  } else if (type == kBucket) {
    return entry.bucket->remove(key);
  } else if (type == kNode) {
    return entry.child->remove(key);;
  } else {
    return false;
  }
}

template<typename KT, typename VT>
RebuildInfo<KT, VT>* TNodePara<KT, VT>::insert(KVT kv, uint32_t depth, 
                                               HyperParameter& hyper_para) {
  uint32_t idx = std::min(std::max(model->predict(kv.first), 0L), 
                          static_cast<int64_t>(capacity - 1));
  uint8_t type = entry_type(idx);
  Entry<KT, VT> entry;
  entries[idx].read(entry);
  if (type == kNone) {
    COUT_W_LOCK("T0: Insert [" << std::fixed << kv.first << "] into the " << idx << "th slot of " << id << "th node")
    set_entry_type(idx, kData);
    entries[idx].update(Entry<KT, VT>(kv));
    return nullptr;
  } else if (type == kData || type == kBucket) {
    if (type == kData) {
      set_entry_type(idx, kBucket);
      entry.bucket = new Bucket<KT, VT>(&entry.kv, 1, hyper_para.max_bucket_size, id, idx);
      entries[idx].update(entry);
      COUT_W_LOCK("T0: Build the " << idx << "th slot into a bucket [" << entry.bucket << "] of " << id << "th node [" << this << "]")
    }
    bool need_rebuild = entry.bucket->insert(kv, hyper_para.max_bucket_size);
    if (need_rebuild) {
      COUT_W_LOCK("T0: Prepare background task of rebuilding the " << idx << "th bucket [" << entry.bucket << "] of " << id << "th node [" << this << "], depth " << depth)
      entries[idx].lock();
      return new RebuildInfo(this, depth, idx, hyper_para);
    } else {
      COUT_W_LOCK("T0: Insert [" << std::fixed << kv.first << "] into the " << idx << "th bucekt")
      return nullptr;
    }
  } else {
    return entry.child->insert(kv, depth + 1, hyper_para);
  }
}

template<typename KT, typename VT>
bool TNodePara<KT, VT>::locked(uint32_t idx) {
  return this->bitmap_lock[idx];
}

template<typename KT, typename VT>
void TNodePara<KT, VT>::lock(uint32_t idx) {
  uint8_t unlocked = 0, locked = 1;
  while (unlikely(cmpxchgb((uint8_t *)&this->bitmap_lock[idx], unlocked, locked) !=
                  unlocked))
    ;
}

template<typename KT, typename VT>
void TNodePara<KT, VT>::unlock(uint32_t idx) {
  bitmap_lock[idx] = 0;
}

template<typename KT, typename VT>
uint8_t TNodePara<KT, VT>::entry_type(uint32_t idx) {
  uint32_t bit_idx = BIT_IDX(idx);
  uint32_t bit_pos = BIT_POS(idx);
  lock(bit_idx);
  uint8_t bit0 = GET_BIT(bitmap0[bit_idx], bit_pos);
  uint8_t bit1 = GET_BIT(bitmap1[bit_idx], bit_pos);
  unlock(bit_idx);
  return (bit0 | (bit1 << 1));
}

template<typename KT, typename VT>
void TNodePara<KT, VT>::set_entry_type(uint32_t idx, uint8_t type) {
  uint32_t bit_idx = BIT_IDX(idx);
  uint32_t bit_pos = BIT_POS(idx);
  lock(bit_idx);
  if (GET_BIT(bitmap0[bit_idx], bit_pos) ^ GET_BIT(type, 0)) {
    REV_BIT(bitmap0[bit_idx], bit_pos);
  }
  if (GET_BIT(bitmap1[bit_idx], bit_pos) ^ GET_BIT(type, 1)) {
    REV_BIT(bitmap1[bit_idx], bit_pos);
  }
  unlock(bit_idx);
}

template<typename KT, typename VT>
void TNodePara<KT, VT>::destroy_self() {    
  delete model;
  model = nullptr;
  for (uint32_t i = 0; i < capacity; ++ i) {
    uint8_t type_i = entry_type(i);
    Entry<KT, VT> entry_i;
    entries[i].read(entry_i);
    if (type_i == kBucket) {
      delete entry_i.bucket;
    } else if (type_i == kNode) {
      uint32_t j = i;
      for (; j < capacity; ++ j) {
        uint8_t type_j = entry_type(j);
        Entry<KT, VT> entry_j;
        entries[j].read(entry_j);
        if (type_j != kNode || 
            entry_j.child != entry_i.child) {
          break;
        }
      }
      delete entry_i.child;
      i = j - 1;
    }
  }
  delete[] bitmap0;
  bitmap0 = nullptr;
  delete[] bitmap1;
  bitmap1 = nullptr;
  if (entries != nullptr) {
    delete[] entries;
    entries = nullptr;
  }
  capacity = 0;
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
  entries = new AtomicVal<Entry<KT, VT>>[ci->max_size];
  bitmap_lock = new uint8_t[bit_len];
  memset(bitmap0, 0, sizeof(BIT_TYPE) * bit_len);
  memset(bitmap1, 0, sizeof(BIT_TYPE) * bit_len);
  for (uint32_t i = 0; i < bit_len; ++ i) {
    bitmap_lock[i] = 0;
  }
  // Recursively build the node
  for (uint32_t i = 0, j = 0; i < ci->num_conflicts; ++ i) {
    uint32_t p = ci->positions[i];
    uint32_t c = ci->conflicts[i];
    if (c == 0) {
      continue;
    } else if (c == 1) {
      set_entry_type(p, kData);
      entries[p].update(Entry<KT, VT>(kvs[j]));
      // COUT_W_LOCK("T0: build " << p << "th slot of " << id << "th node as a data entry")
      j = j + c;
    } else if (c <= hyper_para.max_bucket_size) {
      set_entry_type(p, kBucket);
      Bucket<KT, VT>* b = new Bucket<KT, VT>(kvs + j, c, 
                                            hyper_para.max_bucket_size, id, p);
      entries[p].update(Entry<KT, VT>(b));
      // COUT_W_LOCK("T0: build " << p << "th slot of " << id << "th node as a bucket [" << b << "]")
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
          TNodePara<KT, VT>* child = new TNodePara<KT, VT>(hyper_para.num_nodes ++);
          entries[p_k].update(Entry<KT, VT>(child));
          child->build(kvs + j, c_k, depth + 1, hyper_para);
          j = j + c_k;
        }
      } else {
        set_entry_type(p, kNode);
        TNodePara<KT, VT>* child = new TNodePara<KT, VT>(hyper_para.num_nodes ++);
        entries[p].update(Entry<KT, VT>(child));
        child->build(kvs + j, seg_size, depth + 1, hyper_para);
        for (uint32_t u = i; u < k; ++ u) {
          uint32_t p_k = ci->positions[u];
          set_entry_type(p_k, kNode);
          entries[p_k].update(Entry<KT, VT>(child));
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