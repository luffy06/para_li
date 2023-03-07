#ifndef BNAF_H
#define BNAF_H

#include "core/common.h"

#include <mkl.h>
#include <mkl_cblas.h>

namespace aflipara {

template<typename KT, typename VT>
class BNAF_Infer {
typedef std::pair<KT, VT> KVT;
typedef std::pair<double, KVT> KKVT;
public:
  int num_layers;
  MKL_INT batch_size;
  MKL_INT in_dim;
  MKL_INT hidden_dim;
  double** weights;
  // 1: in_dim * hidden_dim
  // 2: hidden_dim * hidden_dim
  // ....
  // n: hidden_dim * in_dim
  double* inputs;
  double* outputs[2];
public:
  BNAF_Infer() : inputs(nullptr), weights(nullptr) {
    outputs[0] = nullptr;
    outputs[1] = nullptr;
  }

  ~BNAF_Infer() {
    for (int i = 0; i < num_layers; ++ i) {
      if (weights[i] != nullptr) {
        mkl_free(weights[i]);
      }
    }
    if (inputs != nullptr) {
      mkl_free(inputs);
    }
    if (outputs[0] != nullptr) {
      mkl_free(outputs[0]);
    }
    if (outputs[1] != nullptr) {
      mkl_free(outputs[1]);
    }
  }

  uint64_t model_size() {
    return 0;
  }

  uint64_t size() {
    return sizeof(BNAF_Infer<KT, VT>) + sizeof(double*) * num_layers 
          + sizeof(double) * (batch_size * in_dim + batch_size * hidden_dim * 2)
          + sizeof(double) * (in_dim * hidden_dim * 2 + (num_layers - 2) * hidden_dim * hidden_dim);
  }

  void set_batch_size(uint32_t bs) {
    batch_size = bs;
    if (inputs != nullptr) {
      mkl_free(inputs);
    }
    if (outputs[0] != nullptr) {
      mkl_free(outputs[0]);
    }
    if (outputs[1] != nullptr) {
      mkl_free(outputs[1]);
    }
    inputs= (double*)mkl_calloc(batch_size * in_dim, sizeof(double), 64);
    outputs[0] = (double*)mkl_calloc(batch_size * hidden_dim, sizeof(double), 64);
    outputs[1] = (double*)mkl_calloc(batch_size * hidden_dim, sizeof(double), 64);
  }

  void transform(KKVT* tran_kvs, uint32_t size) {
    prepare_inputs(tran_kvs, size);
    forward();
    prepare_outputs(tran_kvs, size);
  }
  void print_parameters() {
    std::cout << "Layers\t" << num_layers << std::endl;
    std::cout << "Input Dim\t" << in_dim << std::endl;
    std::cout << "Hidden Dim\t" << hidden_dim << std::endl;
    for (int i = 0; i < num_layers; ++ i) {
      print_weight_matrix(i);
    }
  }

private:
  void prepare_inputs(const KKVT* tran_kvs, uint32_t size) {
    if (in_dim == 1) {
      for (uint32_t i = 0; i < size; ++ i) {
        inputs[i] = tran_kvs[i].first;
      }
    } else if (in_dim == 2) {
      for (uint32_t i = 0; i < size; ++ i) {
        inputs[2 * i] = tran_kvs[i].first;
        inputs[2 * i + 1] = tran_kvs[i].first - std::floor(inputs[2 * i]);
      }
    } else if (in_dim == 4) {
      for (uint32_t i = 0; i < size; ++ i) {
        inputs[4 * i] = tran_kvs[i].first;
        inputs[4 * i + 1] = std::floor(inputs[2 * i]);
        double tmp = (tran_kvs[i].first - inputs[4 * i + 1]) * 1000000;
        inputs[4 * i + 2] = std::floor(tmp);
        inputs[4 * i + 3] = tmp - inputs[4 * i + 2];
      }
    } else {
      std::cout << "Unsupported dimensions\t" << in_dim << std::endl;
      exit(-1);
    }
  }

  void prepare_outputs(KKVT* tran_kvs, uint32_t size) {
    if (in_dim == 1) {
      for (uint32_t i = 0; i < size; ++ i) {
        tran_kvs[i] = {inputs[i], tran_kvs[i].second};
      }
    } else if (in_dim == 2) {
      for (uint32_t i = 0; i < size; ++ i) {
        tran_kvs[i] = {inputs[i * 2] + inputs[i * 2 + 1], tran_kvs[i].second};
      }
    } else if (in_dim == 4) {
      for (uint32_t i = 0; i < size; ++ i) {
        tran_kvs[i] = {inputs[i * 4] + inputs[i * 4 + 1] + inputs[i * 4 + 2] + inputs[i * 4 + 3], tran_kvs[i].second};
      }
    } else {
      std::cout << "Unsupported dimensions\t" << in_dim << std::endl;
      exit(-1);
    }
  }

  void forward() {
    // print_outputs(-1, inputs, batch_size, in_dim);
    // Compute the formula: 
    //            alpha * mat_a [m * k] * mat_b [k * n] + beta * mat_c [m * n]
    // cblas_dgemm(layout, trans_a, trans_b, m, n, k, alpha, mat_a, lda, 
    //              mat_b, ldb, beta, mat_c, ldc)
    // IN [batch_size * in_dim] * W_0 [in_dim * hidden_dim] = 
    // OUT [batch_size * hidden_dim]
    cblas_dgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, 
                batch_size, hidden_dim, in_dim, 
                1, inputs, in_dim, 
                weights[0], hidden_dim, 
                0, outputs[0], hidden_dim);
    // print_weight_matrix(0);
    // print_outputs(0, outputs[0], batch_size, hidden_dim);
    vdTanh(batch_size * hidden_dim, outputs[0], outputs[1]);
    // print_outputs(0, outputs[1], batch_size, hidden_dim);
    for (int i = 1; i < num_layers - 1; ++ i) {
      // IN [batch_size * hidden_dim] * W_i [hidden_dim * hidden_dim] = 
      // OUT [batch_size * hidden_dim]
      cblas_dgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, 
                  batch_size, hidden_dim, hidden_dim, 
                  1, outputs[1], hidden_dim, 
                  weights[i], hidden_dim, 
                  0, outputs[0], hidden_dim);
      // print_weight_matrix(i);
      // print_outputs(i, outputs[0], batch_size, hidden_dim);      
      vdTanh(batch_size * hidden_dim, outputs[0], outputs[1]);
      // print_outputs(i, outputs[1], batch_size, hidden_dim);      
    }
    // IN [batch_size * hidden_dim] * W_L [hidden_dim * in_dim] = 
    // OUT [batch_size * in_dim]
    cblas_dgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, 
                batch_size, in_dim, hidden_dim, 
                1, outputs[1], hidden_dim, 
                weights[num_layers - 1], in_dim, 
                0, inputs, in_dim);
    // print_weight_matrix(num_layers);
    // print_outputs(num_layers, inputs, batch_size, in_dim);
  }

  void print_weight_matrix(int l) {
    if (l == 0) {
      std::cout << std::fixed << "Weight (0)" << std::endl;
      for (int i = 0; i < in_dim; ++ i) {
        for (int j = 0; j < hidden_dim; ++ j) {
          std::cout << std::fixed << weights[0][i * hidden_dim + j] << "\t";
        }
        std::cout << std::endl;
      }
    } else if (l == num_layers) {
      std::cout << std::fixed << "Weight (" << num_layers << ")" << std::endl;
      for (int i = 0; i < hidden_dim; ++ i) {
        for (int j = 0; j < in_dim; ++ j) {
          std::cout << std::fixed << weights[num_layers][i * in_dim + j] << "\t";
        }
        std::cout << std::endl;
      }    
    } else {
      std::cout << std::fixed << "Weight (" << l << ")" << std::endl;
      for (int i = 0; i < hidden_dim; ++ i) {
        for (int j = 0; j < hidden_dim; ++ j) {
          std::cout << std::fixed << weights[l][i * hidden_dim + j] << "\t";
        }
        std::cout << std::endl;
      }
    }
  }

  void print_outputs(int idx, double* outputs, int num_rows, int num_columns) {
    if (idx == -1) {
      std::cout << "Input" << std::endl;
    } else {
      std::cout << "Output (" << idx << ")" << std::endl;
    }
    // std::cout << "Shape\t" << num_rows << "*" << num_columns << std::endl;
    for (int i = 0; i < num_rows; ++ i) {
      for (int j = 0; j < num_columns; ++j) {
        std::cout << std::fixed << std::setprecision(std::numeric_limits<double>::digits10) 
                  << outputs[i * num_columns + j] << "\t";
      }
      std::cout << std::endl;
    }
  }

};

}

#endif