#ifndef AFLI_PARA_H
#define AFLI_PARA_H

#include "core/afli_node_para_impl.h"

namespace aflipara {

template<typename KT, typename VT>
class AFLIPara {
typedef std::pair<KT, VT> KVT;
private:
  TNodePara<KT, VT>* volatile root;
  boost::asio::thread_pool* pool;
  bool self_pool = false;
public:
  HyperParameter hyper_para;
public:
  AFLIPara(uint32_t num_bg=1, boost::asio::thread_pool* p=nullptr);
  ~AFLIPara();

  void bulk_load(const KVT* kvs, uint32_t size);
  bool find(KT key, VT& value);
  bool remove(KT key);
  bool update(KVT kv);
  void insert(KVT kv);
  uint32_t scan(KT begin, KT end, std::vector<KVT>& res);

  uint64_t model_size();
  uint64_t index_size();

  void print_statistics();
private:
  static void rebuild(AFLIBGParam<KT, VT>* args);

  void adapt_bucket_size(const KVT* kvs, uint32_t size, 
                         HyperParameter& hyper_para);
  // uint32_t collect_tree_statistics(const TNodePara<KT, VT>* node, 
  //                                  uint32_t depth, TreeStat& ts);
};

}
#endif