#ifndef BUCKET_PARA_H
#define BUCKET_PARA_H

#include "util/common.h"

namespace aflipara {

template<typename KT, typename VT>
class Bucket {
typedef std::pair<KT, VT> KVT;
public:
  uint32_t node_id = 531;
  uint32_t idx = 531;
  KVT* data;
  uint8_t size;
  volatile uint8_t status = 0;

public:
  Bucket() = delete;
  explicit Bucket(const KVT* kvs, uint32_t size, const uint32_t capacity, uint32_t a, uint32_t b);
  ~Bucket();

  uint8_t get_size();
  KVT* copy();
  
  bool find(KT key, VT& value);
  bool update(KVT kv);
  bool remove(KT key);
  bool insert(KVT kv, const uint32_t capacity);

private:
  void lock();
  void unlock();

};

}

#endif