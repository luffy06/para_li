#ifndef CONFLICTS_PARA_H
#define CONFLICTS_PARA_H

#include "core/linear_model.h"
#include "core/common.h"

namespace aflipara {

struct ConflictsInfo {
  uint32_t* conflicts;
  uint32_t* positions;
  uint32_t num_conflicts;
  uint32_t max_size;
  uint32_t size;

  ConflictsInfo(uint32_t size, uint32_t max_size) : max_size(max_size), 
                num_conflicts(0) {
    size = size;
    conflicts = new uint32_t[size];
    positions = new uint32_t[size];
  }

  ~ConflictsInfo() {
    delete[] conflicts;
    delete[] positions;
  }

  void add_conflict(uint32_t position, uint32_t conflict) {
    conflicts[num_conflicts] = conflict;
    positions[num_conflicts] = position;
    num_conflicts ++;
  }
};

template<typename KT, typename VT>
ConflictsInfo* build_linear_model(const std::pair<KT, VT>* kvs, uint32_t size,
                                  LinearModel*& model, double size_amp=1) {
  if (model != nullptr) {
    model->slope = model->intercept = 0;
  } else {
    model = new LinearModel();
  }
  // OPT: Find a linear regression model that has the minimum conflict degree
  // The heuristics below is a simple method that scales the positions
  KT min_key = kvs[0].first;
  KT max_key = kvs[size - 1].first;
  ASSERT_WITH_MSG(!equal(min_key, max_key), "Range [" << min_key << ", " 
                  << max_key << "], Size: " << size 
                  << ", all keys used to build the linear model are the same.")
  int64_t capacity = static_cast<int64_t>(size * size_amp);
  double key_space = (max_key - min_key) / static_cast<double>(capacity);
  LinearModelBuilder builder;
  for (uint32_t i = 0; i < size; ++ i) {
    double key = (kvs[i].first - min_key) / key_space;
    // double key = kvs[i].first;
    double y = i;
    builder.add(key, y);
  }
  builder.build(model);
  if (equal(model->slope, 0.)) {
    // Fail to build a linear model
    COUT_ERR("Fail to build a linear model, since the slope is zero and the " 
             << "keys ranging from [" << min_key << "] to [" << max_key 
             << "] are too large.")
  } else {
    // y = k * z + b
    // z = (x - u) / s
    // y = k * (x - u) / s + b = (k / s) * x - k * u / s + b
    model->slope = model->slope / key_space;
    model->intercept = -model->slope * min_key + 0.5;
    ASSERT_WITH_MSG(model->predict(min_key) == 0, 
                    "The first prediction must be zero")
    int64_t predicted_size = model->predict(max_key) + 1;
    if (predicted_size > 1) {
      capacity = std::min(predicted_size, capacity);
    }
    int64_t first_pos = std::min(std::max(model->predict(min_key), 0L), 
                                          capacity - 1);
    int64_t last_pos = std::min(std::max(model->predict(max_key), 0L), 
                                         capacity - 1);
    if (last_pos == first_pos) {
      // Model fails to predict since all predicted positions are rounded to 
      // the same one
      COUT_INFO("The last predicted position [" << last_pos 
                << "] is the same as the first predicted position [" 
                << first_pos << "]");
      model->slope = size / key_space;
      model->intercept = -model->slope * min_key + 0.5;
    }
    ConflictsInfo* ci = new ConflictsInfo(size, capacity);
    int64_t p_last = first_pos;
    uint32_t conflict = 1;
    for (uint32_t i = 1; i < size; ++ i) {
      double key = kvs[i].first;
      int64_t p = std::min(std::max(model->predict(key), 0L), capacity - 1);
      if (p == p_last) {
        conflict ++;
      } else {
        ci->add_conflict(p_last, conflict);
        p_last = p;
        conflict = 1;
      }
    }
    if (conflict > 0) {
      ci->add_conflict(p_last, conflict);
    }
    return ci;
  }
}

template<typename KT, typename VT>
uint32_t compute_tail_conflicts(const std::pair<KT, VT>* kvs, uint32_t size, 
                                double size_amp, float kTailPercent=0.99) {
  // The input keys should be ordered
  LinearModel* model = new LinearModel();
  ConflictsInfo* ci = build_linear_model<KT, VT>(kvs, size, model, size_amp);
  delete model;

  if (ci->num_conflicts == 0) {
    delete ci;
    return 0;
  } else {
    std::sort(ci->conflicts, ci->conflicts + ci->num_conflicts);
    int32_t tail_idx = int32_t(ci->num_conflicts * kTailPercent) - 1;
    uint32_t tail_conflicts = ci->conflicts[std::max(0, tail_idx)];
    delete ci;
    return tail_conflicts - 1;
  }
}

}
#endif