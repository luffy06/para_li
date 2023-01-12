#ifndef BUCKET_PARA_IMPL_H
#define BUCKET_PARA_IMPL_H

#include "afli_para/bucket.h"

namespace aflipara {

template<typename KT, typename VT>
Bucket<KT, VT>::Bucket(const KVT* kvs, uint32_t s, const uint32_t capacity, 
                       int a, int b) {
  size = s;
  data = new KVT[capacity + 1];
  node_id = a;
  idx = b;
  for (uint32_t i = 0; i < s; ++ i) {
    data[i] = kvs[i];
  }
}

template<typename KT, typename VT>
Bucket<KT, VT>::~Bucket() {
  if (data != nullptr) {
    delete[] data;
    data = nullptr;
  }
}

template<typename KT, typename VT>
uint8_t Bucket<KT, VT>::get_size() {
  return size;
}

template<typename KT, typename VT>
std::pair<KT, VT>* Bucket<KT, VT>::copy() {
  KVT* kvs = new KVT[size];
  for (uint32_t i = 0; i < size; ++ i) {
    kvs[i] = data[i];
  }
  return kvs;
}

template<typename KT, typename VT>
bool Bucket<KT, VT>::find(KT key, VT& value) {
  lock();
  bool found = false;
  for (uint32_t i = 0; i < size; ++ i) {
    if (compare(data[i].first, key)) {
      value = data[i].second;
      found = true;
      break;
    }
  }
  unlock();
  return found;
}

template<typename KT, typename VT>
bool Bucket<KT, VT>::update(KVT kv) {
  lock();
  bool found = false;
  for (uint8_t i = 0; i < size; ++ i) {
    if (compare(data[i].first, kv.first)) {
      data[i] = kv;
      found = true;
      break;
    }
  }
  unlock();
  return found;
}

template<typename KT, typename VT>
bool Bucket<KT, VT>::remove(KT key) {
  lock();
  bool found = false;
  for (uint8_t i = 0; i < size; ++ i) {
    if (compare(data[i].first, key)) {
      found = true;
    }
    if (found && i + 1 < size) {
      data[i] = data[i + 1];
    }
  }
  size -= found;
  unlock();
  return found;
}

template<typename KT, typename VT>
bool Bucket<KT, VT>::insert(KVT kv, const uint32_t capacity) {
  lock();
  data[size] = kv;
  size ++;
  bool need_rebuild = !(size < capacity);
  unlock();
  return need_rebuild;
}

template<typename KT, typename VT>
void Bucket<KT, VT>::lock() {
  uint8_t unlocked = 0, locked = 1;
  while (unlikely(cmpxchgb((uint8_t *)&this->status, unlocked, locked) !=
                  unlocked)) ;
}

template<typename KT, typename VT>
void Bucket<KT, VT>::unlock() {
  status = 0;
}

}
#endif