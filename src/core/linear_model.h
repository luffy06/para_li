#ifndef LINEAR_MODEL_PARA_H
#define LINEAR_MODEL_PARA_H

#include "common.h"

namespace aflipara {

template<class KT>
class LinearModel {
 public:
  double slope;
  double intercept;

  LinearModel() : slope(0), intercept(0) { }

  inline int64_t predict(KT key) const {
    return static_cast<int64_t>(std::floor(slope * key + intercept));
  }

  inline double predict_double(KT key) const {
    return slope * static_cast<double>(key) + intercept;
  }
};

template<class KT>
class LinearModelBuilder {
 public:
  int32_t count;
  double x_sum;
  double y_sum;
  double xx_sum;
  double xy_sum;
  KT x_min;
  KT x_max;
  double y_min;
  double y_max;

  LinearModelBuilder() : count(0), x_sum(0), y_sum(0), xx_sum(0), xy_sum(0), 
                         x_min(std::numeric_limits<KT>::max()), 
                         x_max(std::numeric_limits<KT>::lowest()),
                         y_min(std::numeric_limits<double>::max()), 
                         y_max(std::numeric_limits<double>::lowest()) { }

  inline void add(KT x, double y) {
    count++;
    x_sum += static_cast<double>(x);
    y_sum += static_cast<double>(y);
    xx_sum += static_cast<double>(x) * x;
    xy_sum += static_cast<double>(x) * y;
    x_min = std::min(x, x_min);
    x_max = std::max(x, x_max);
    y_min = std::min(y, y_min);
    y_max = std::max(y, y_max);
  }

  // TODO: the calculated slope or intercept is too small or too large, the 
  // precision is lost.
  void build(LinearModel<KT> *lrm) {
    if (count <= 1) {
      lrm->slope = 0;
      lrm->intercept = static_cast<double>(y_sum);
      return;
    }

    if (equal(static_cast<double>(count) * xx_sum, x_sum * x_sum)) {
      // all values in a bucket have the same key.
      lrm->slope = 0;
      lrm->intercept = static_cast<double>(y_sum) / count;
      return ;
    }

    auto slope = static_cast<double>(
                 (static_cast<double>(count) * xy_sum - x_sum * y_sum) /
                 (static_cast<double>(count) * xx_sum - x_sum * x_sum));
    auto intercept = static_cast<double>(
                     (y_sum - static_cast<double>(slope) * x_sum) / count);
    lrm->slope = slope;
    lrm->intercept = intercept;

    // If floating point precision errors, fit spline
    if (lrm->slope <= 0) {
      lrm->slope = 1. * (y_max - y_min) / (x_max - x_min);
      lrm->intercept = -static_cast<double>(x_min) * lrm->slope;
    }
  }
};

}
#endif