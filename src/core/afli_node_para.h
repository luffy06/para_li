#ifndef AFLI_NODE_PARA_H
#define AFLI_NODE_PARA_H

#include "core/bucket_impl.h"
#include "core/conflicts.h"
#include "core/linear_model.h"
#include "core/common.h"

namespace aflipara {

template<typename KT, typename VT>
class AFLIPara;

template<typename KT, typename VT>
class TNodePara;

struct HyperParameter {
  // Parameters
  uint32_t max_bucket_size = 6;
  uint32_t aggregate_size = 0;
  uint32_t max_num_bg = 2;
  uint32_t num_nodes = 0;
  // Constant parameters
  const uint32_t kMaxBucketSize = 6;
  const uint32_t kMinBucketSize = 1;
  const double kSizeAmplification = 1;
  const double kTailPercent = 0.99;
};

enum EntryType {
  kNone   = 0,
  kData   = 1,
  kBucket = 2,
  kNode   = 3
};

template<typename KT, typename VT>
union Entry {
  Bucket<KT, VT>*     bucket;      // The bucket pointer
  TNodePara<KT, VT>*  child;       // The child node pointer
  std::pair<KT, VT>   kv;          // The key-value pair

  Entry() {
    memset(this, 0, sizeof(Entry));
  }
};

template<typename KT, typename VT>
struct AFLIBGParam {
  TNodePara<KT, VT>*          node_ptr;
  uint32_t                    depth;
  uint32_t                    idx;
  HyperParameter&             hyper_para;

  AFLIBGParam(TNodePara<KT, VT>* a, uint32_t b, uint32_t c, HyperParameter& d) 
              : node_ptr(a), depth(b), idx(c), hyper_para(d) { }
};

template<typename KT, typename VT>
class TNodePara {
typedef std::pair<KT, VT> KVT;
public:
  uint32_t                    id;        // DELETE

  LinearModel<KT>*            model;     // 'nullptr' means this is a btree node
  uint32_t                    capacity;  // The pre-allocated size of array
  uint8_t*                    bitmap0;   // The i-th bit indicates whether the i-th position has a bucket or a child node
  uint8_t*                    bitmap1;   // The i-th bit indicates whether the i-th position is a bucket
  Entry<KT, VT>*              entries;   // The pointer array that stores the pointer of buckets or child nodes
  
  volatile uint8_t*           entry_lock;
  volatile uint8_t            node_lock;

  friend class AFLIPara<KT, VT>;
public:
  // Constructor and deconstructor
  explicit TNodePara(uint32_t id);
  ~TNodePara();

  inline uint32_t get_capacity();

  // User API interfaces
  bool find(KT key, VT& value, uint32_t depth=1);
  bool remove(KT key);
  bool update(KVT kv);
  AFLIBGParam<KT, VT>* insert(KVT kv, uint32_t depth, 
                              HyperParameter& hyper_para);

private:
  bool node_locked();
  void lock_node();
  void unlock_node();

  bool entry_locked(uint32_t idx);
  void lock_entry(uint32_t idx);
  void unlock_entry(uint32_t idx);

  uint8_t entry_type(uint32_t idx);
  void set_entry_type(uint32_t idx, uint8_t type);

  void destroy_self();
  void build(const KVT* kvs, uint32_t size, uint32_t depth, 
             HyperParameter& hyper_para);
};

}
#endif