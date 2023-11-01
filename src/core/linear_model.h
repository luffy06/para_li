#ifndef LINEAR_MODEL_PARA_H
#define LINEAR_MODEL_PARA_H

#include "core/common.h"

namespace aflipara {

class LinearModel {
 public:
  double slope;
  double intercept;

  LinearModel() : slope(0), intercept(0) { }

  inline int64_t predict(double key) const {
    return static_cast<int64_t>(std::floor(slope * key + intercept));
  }

  inline double predict_double(double key) const {
    return slope * static_cast<double>(key) + intercept;
  }
};

class LinearModelBuilder {
 public:
  uint32_t count;
  double x_sum;
  double y_sum;
  double xx_sum;
  double xy_sum;
  double x_min;
  double x_max;
  double y_min;
  double y_max;

  LinearModelBuilder() : count(0), x_sum(0), y_sum(0), xx_sum(0), xy_sum(0), 
                         x_min(std::numeric_limits<double>::max()), 
                         x_max(std::numeric_limits<double>::lowest()),
                         y_min(std::numeric_limits<double>::max()), 
                         y_max(std::numeric_limits<double>::lowest()) { }

  inline void add(double x, double y) {
    count++;
    x_sum += static_cast<double>(x);
    y_sum += y;
    xx_sum += static_cast<double>(x) * x;
    xy_sum += static_cast<double>(x) * y;
    x_min = std::min(x, x_min);
    x_max = std::max(x, x_max);
    y_min = std::min(y, y_min);
    y_max = std::max(y, y_max);
  }

  // TODO: the calculated slope or intercept is too small or too large, the 
  // precision is lost.
  void build(LinearModel* lrm) {
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

    double slope = (static_cast<double>(count) * xy_sum - x_sum * y_sum) 
                   / (static_cast<double>(count) * xx_sum - x_sum * x_sum);
    double intercept = (y_sum - slope * x_sum) / static_cast<double>(count);
    lrm->slope = slope;
    lrm->intercept = intercept;

    // If floating point precision errors, fit spline
    if (lrm->slope <= 0) {
      lrm->slope = (y_max - y_min) / static_cast<double>(x_max - x_min);
      lrm->intercept = -static_cast<double>(x_min) * lrm->slope;
    }
  }
};

}
#endif