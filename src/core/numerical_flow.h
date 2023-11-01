#ifndef NUMERICAL_FLOW_H
#define NUMERICAL_FLOW_H

#include "core/bnaf.h"
#include "core/common.h"

namespace aflipara {

template<typename KT, typename VT>
class NumericalFlow {
typedef std::pair<KT, VT> KVT;
typedef std::pair<double, KVT> KKVT;
public:
  double mean;
  double var;
  MKL_INT batch_size;
  BNAF_Infer<KT, VT> model;

public:
  explicit NumericalFlow(std::string weight_path, uint32_t bs) 
    : batch_size(bs) {
    load(weight_path);
    model.set_batch_size(bs);
  }

  uint64_t size() {
    return sizeof(NumericalFlow<KT, VT>) - sizeof(BNAF_Infer<KT, VT>) + model.size();
  }

  void set_batch_size(uint32_t bs) {
    batch_size = bs;
    model.set_batch_size(batch_size);
  }

  void transform(const KVT* kvs, uint32_t size, KKVT* tran_kvs) {
    for (uint32_t i = 0; i < size; ++ i) {
      tran_kvs[i] = {(kvs[i].first - mean) / var, kvs[i]};
    }
    uint32_t num_batches = static_cast<uint32_t>(std::ceil(size * 1. / batch_size));
    for (uint32_t i = 0; i < num_batches; ++ i) {
      uint32_t l = i * batch_size;
      uint32_t r = std::min((i + 1) * batch_size, size);
      model.transform(tran_kvs + l, r - l);
    }
  }

  KKVT transform(const KVT& kv) {
    KKVT t_kv = {(kv.first - mean) / var, kv};
    model.transform(&t_kv, 1);
    return t_kv;
  }

private:
  void load(std::string path) {
    std::fstream in(path, std::ios::in);
    if (!in.is_open()) {
      std::cout << "File:" << path << " doesn't exist" << std::endl;
      exit(-1);
    }
    in >> model.in_dim >> model.hidden_dim >> model.num_layers;
    in >> mean >> var;
    model.weights = new double*[model.num_layers];
    for (uint32_t w = 0; w < model.num_layers; ++ w) {
      uint32_t n, m;
      in >> n >> m;
      model.weights[w] = (double*)mkl_calloc(n * m, sizeof(double), 64);
      for (uint32_t i = 0; i < n; ++ i) {
        for (uint32_t j = 0; j < m; ++ j) {
          in >> model.weights[w][i * m + j];
        }
      }
    }
    in.close();
  }

};

}

#endif