#ifndef AFLI_PARA_IMPL_H
#define AFLI_PARA_IMPL_H

#include "core/afli_para.h"

namespace aflipara {

template<typename KT, typename VT>
AFLIPara<KT, VT>::AFLIPara(uint32_t num_bg, boost::asio::thread_pool* p) {
  root = nullptr;
  self_pool = false;
  if (num_bg > 0) {
    if (p == nullptr) {
      self_pool = true;
      pool = new boost::asio::thread_pool(num_bg);
    } else {
      pool = p;
    }
  } else {
    pool = nullptr;
  }
}

template<typename KT, typename VT>
AFLIPara<KT, VT>::~AFLIPara() {
  if (root != nullptr) {
    delete root;
  }
  if (self_pool) {
    delete pool;
  }
}

template<typename KT, typename VT>
void AFLIPara<KT, VT>::bulk_load(const KVT* kvs, uint32_t size) {
  ASSERT_WITH_MSG(root == nullptr, 
                  "The index must be empty before bulk loading");
  root = new TNodePara<KT, VT>(hyper_para.num_nodes ++);
  // adapt_bucket_size(kvs, size, hyper_para);
  root->build(kvs, size, 1, hyper_para);
}

template<typename KT, typename VT>
bool AFLIPara<KT, VT>::find(KT key, VT& value) {
  bool res = root->find(key, value);
  return res;
}

template<typename KT, typename VT>
bool AFLIPara<KT, VT>::remove(KT key) {
  return root->remove(key);
}

template<typename KT, typename VT>
bool AFLIPara<KT, VT>::update(KVT kv) {
  return root->update(kv);
}

template<typename KT, typename VT>
void AFLIPara<KT, VT>::insert(KVT kv) {
  AFLIBGParam<KT, VT>* args = root->insert(kv, 1, hyper_para);
  if (args != nullptr) {
    if (pool != nullptr) {
      boost::asio::post(*pool, boost::bind(AFLIPara<KT, VT>::rebuild, args));
    } else { // No background threads, directly rebuild
      AFLIPara::rebuild(args);
    }
  }
}

template<typename KT, typename VT>
uint32_t AFLIPara<KT, VT>::scan(KT begin, KT end, std::vector<KVT>& res) {
  return 0;
}

template<typename KT, typename VT>
uint64_t AFLIPara<KT, VT>::model_size() {
  // TreeStat ts;
  // ts.bucket_size_ = hyper_para.max_bucket_size;
  // collect_tree_statistics(root, 1, ts);
  // return ts.model_size_;
  return 0;
}

template<typename KT, typename VT>
uint64_t AFLIPara<KT, VT>::index_size() {
  // TreeStat ts;
  // ts.bucket_size_ = hyper_para.max_bucket_size;
  // collect_tree_statistics(root, 1, ts);
  // return ts.index_size_;
  return 0;
}

template<typename KT, typename VT>
void AFLIPara<KT, VT>::print_statistics() {
  // TreeStat ts;
  // ts.bucket_size_ = hyper_para.max_bucket_size;
  // collect_tree_statistics(root, 1, ts);
  // ts.show();
}

template<typename KT, typename VT>
void AFLIPara<KT, VT>::rebuild(AFLIBGParam<KT, VT>* args) {
  TNodePara<KT, VT>* node = args->node_ptr;
  uint32_t depth = args->depth;
  uint32_t idx = args->idx;
  Bucket<KT, VT>* bucket = node->entries[idx].bucket;
  assert(bucket->idx == idx);
  assert(bucket->node_id == node->id);
  uint32_t bucket_size = bucket->get_size();
  KVT* kvs = bucket->copy();
  std::sort(kvs, kvs + bucket_size, 
    [](auto const& a, auto const& b) {
      return a.first < b.first;
  });
  delete bucket;
  TNodePara<KT, VT>* child = new TNodePara<KT, VT>(args->hyper_para.num_nodes++);
  child->build(kvs, bucket_size, args->depth + 1, args->hyper_para);
  node->set_entry_type(idx, kNode);
  node->entries[idx].child = child;
  node->unlock_entry(idx);
  delete[] kvs;
  delete args;
}

template<typename KT, typename VT>
void AFLIPara<KT, VT>::adapt_bucket_size(const KVT* kvs, uint32_t size, 
                                         HyperParameter& hyper_para) {
  uint32_t tail_conflicts = compute_tail_conflicts<KT, VT>(kvs, size, 
                              hyper_para.kSizeAmplification, 
                              hyper_para.kTailPercent);
  tail_conflicts = std::min(hyper_para.kMaxBucketSize, tail_conflicts);
  tail_conflicts = std::max(hyper_para.kMinBucketSize, tail_conflicts);
  hyper_para.max_bucket_size = tail_conflicts;
}

// template<typename KT, typename VT>
// uint32_t AFLIPara<KT, VT>::collect_tree_statistics(const TNodePara<KT, VT>* node, 
//                                   uint32_t depth, TreeStat& ts) {
//   if (node->model_ != nullptr) {
//     // Model node
//     ts.num_model_nodes_ ++;
//     ts.model_size_ += sizeof(TNodePara<KT, VT>) + sizeof(LinearModel);
//     ts.index_size_ += sizeof(TNodePara<KT, VT>) + sizeof(LinearModel) 
//                     + sizeof(BIT_TYPE) * 2 * BIT_LEN(node->capacity_) 
//                     + sizeof(AtomicEntry<KT, VT>) * node->capacity_;
//     bool is_leaf_node = true;
//     uint32_t tot_kvs = 0;
//     uint32_t tot_conflicts = 0;
//     uint32_t num_conflicts = 0;
//     for (uint32_t i = 0; i < node->capacity_; ++ i) {
//       uint8_t type = node->entry_type(i);
//       if (type == kNone) {
//         // Empty slot
//         continue;
//       } else if (type == kData) {
//         // Data slot
//         ts.num_data_model_ ++;
//         ts.sum_depth_ += depth;
//         tot_kvs ++;
//         continue;
//       } else if (type == kBucket) {
//         // Bucket pointer
//         ts.num_buckets_ ++;
//         ts.num_data_bucket_ += node->entries_[i].bucket->size;
//         ts.model_size_ += sizeof(Bucket<KT, VT>);
//         ts.index_size_ += sizeof(Bucket<KT, VT>) 
//                         + sizeof(KVT) * hyper_para.max_bucket_size;
//         ts.sum_depth_ += (depth + 1) * node->entries_[i].bucket->size;
//         tot_kvs += node->entries_[i].bucket->size;
//         tot_conflicts += node->entries_[i].bucket->size - 1;
//         num_conflicts ++;
//       } else if (type == kNode) {
//         // Child node pointer
//         uint32_t num_kvs_child = collect_tree_statistics(node->entries_[i].child, depth + 1, ts);
//         tot_conflicts += num_kvs_child;
//         num_conflicts ++;
//         is_leaf_node = false;
//         // Find the duplicated child node pointers
//         uint32_t j = i + 1;
//         for (; j < node->size; ++ j, num_conflicts ++) {
//           uint8_t type_j = node->entry_type(j);
//           if (type_j != kNode 
//               || node->entries_[j].child != node->entries_[i].child) {
//             break;
//           }
//         }
//         i = j - 1;
//       }
//     }
//     ts.node_conflicts_ += num_conflicts ? tot_conflicts * 1. / num_conflicts : 0;
//     if (is_leaf_node) {
//       ts.num_leaf_nodes_ ++;
//       ts.max_depth_ = std::max(ts.max_depth_, depth);
//     }
//     return tot_conflicts;
//   } else {
//     // Dense node
//     ts.num_dense_nodes_ ++;
//     ts.num_data_dense_ ++;
//     uint32_t tot_conflicts = 0;
//     for (uint32_t i = 1; i < node->size; ++ i) {
//       if (!EQ(node->entries_[i].kv_.first, 
//               node->entries_[i - 1].kv_.first)) {
//         ts.num_data_dense_ ++;
//         tot_conflicts ++;
//       }
//     }
//     ts.sum_depth_ += depth * tot_conflicts;
//     ts.node_conflicts_ += tot_conflicts;
//     ts.model_size_ += sizeof(TNodePara<KT, VT>);
//     ts.index_size_ += sizeof(TNodePara<KT, VT>) 
//                     + sizeof(AtomicEntry<KT, VT>) * node->capacity_;
//     ts.num_leaf_nodes_ ++;
//     ts.sum_depth_ += depth;
//     ts.max_depth_ = std::max(ts.max_depth_, depth);
//     return tot_conflicts;
//   }
//   return 0;
// }

}
#endif