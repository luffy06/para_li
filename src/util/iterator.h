#ifndef ITERATOR_PARA_H
#define ITERATOR_PARA_H

#include "util/common.h"

namespace aflipara {

template<typename KT, typename VT>
class ResultIterator {
typedef std::pair<KT, VT> KVT;
private:
  KVT kv_;
  bool end;

public:
  ResultIterator() : end(true) { }

  ResultIterator(KVT kv) : kv_(kv), end(false) { }

  bool is_end() { return end; }

  KT key() { return kv_.first; }

  VT value() { return kv_.second; }

  VT* value_addr() { return &kv_.second; }

  KVT kv() { return kv_; }

  ResultIterator<KT, VT>& operator=(const ResultIterator<KT, VT>& other) {
    if (this != &other) {
      kv_ = other.kv_;
    }
    return *this;
  }
};

}
#endif