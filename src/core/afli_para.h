#ifndef AFLI_PARA_H
#define AFLI_PARA_H

#include "afli_node_para_impl.h"
#include "BS_thread_pool.hpp"

namespace aflipara {

template<typename KT, typename VT>
struct BGInfo {
  void* index_ptr;
  volatile bool running;
};

template<typename KT, typename VT>
class AFLIPara {
typedef std::pair<KT, VT> KVT;
private:
  TNodePara<KT, VT>* volatile root;
  BS::thread_pool* pool;
public:
  HyperParameter hyper_para;
public:
  AFLIPara(uint32_t num_bg=1);
  ~AFLIPara();

  void bulk_load(const KVT* kvs, uint32_t size);
  bool find(KT key, VT& value);
  bool update(KVT kv);
  bool remove(KT key);
  void insert(KVT kv);
  uint32_t scan(KT begin, KT end, std::vector<KVT>& res);

  uint64_t model_size();
  uint64_t index_size();

  void print_statistics();
private:
  void rebuild(RebuildInfo<KT, VT>* ri);

  void adapt_bucket_size(const KVT* kvs, uint32_t size, 
                         HyperParameter& hyper_para);
  // uint32_t collect_tree_statistics(const TNodePara<KT, VT>* node, 
  //                                  uint32_t depth, TreeStat& ts);
};

}
#endif