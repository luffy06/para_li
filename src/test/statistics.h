#include "common.h"

struct TreeStat {
  // Configs
  uint32_t bucket_size_ = 0;
  uint32_t max_aggregate_ = 0;
  // Node counts
  uint32_t num_model_nodes_ = 0;
  uint32_t num_buckets_ = 0;
  uint32_t num_dense_nodes_ = 0;
  // Data counts
  uint32_t num_data_model_ = 0;
  uint32_t num_data_bucket_ = 0;
  uint32_t num_data_dense_ = 0;
  // Depth
  uint32_t num_leaf_nodes_ = 0;
  uint32_t sum_depth_ = 0;
  uint32_t max_depth_ = 0;
  // Size
  uint64_t model_size_ = 0;
  uint64_t index_size_ = 0;
  // Conflicts
  double node_conflicts_ = 0;

  uint32_t num_data() {
    return num_data_model_ + num_data_bucket_ + num_data_dense_;
  }

  double avg_depth() {
    return sum_depth_ * 1. / num_data();
  }

  void show() {
    std::cout << std::string(15, '#') << "Tree Statistics" 
              << std::string(15, '#') << std::endl;
    std::cout << "Bucket Threshold\t" << bucket_size_ << std::endl;
    std::cout << "Max Aggregate Size\t" << max_aggregate_ << std::endl;
    std::cout << "Number of Model Nodes\t" << num_model_nodes_ << std::endl;
    std::cout << "Number of Buckets\t" << num_buckets_ << std::endl;
    std::cout << "Number of Dense Nodes\t" << num_dense_nodes_ << std::endl;
    std::cout << "Number of Data\t" << num_data() << std::endl;
    std::cout << "Number of Data in Model Nodes\t" << num_data_model_ 
              << std::endl;
    std::cout << "Number of Data in Buckets\t" << num_data_bucket_ << std::endl;
    std::cout << "Number of Data in Dense Nodes\t" << num_data_dense_ 
              << std::endl;
    std::cout << "Average Depth\t" << avg_depth() << std::endl;
    std::cout << "Maximum Depth\t" << max_depth_ << std::endl;
    std::cout << "Model Size\t" << model_size_ << std::endl;
    std::cout << "Index Size\t" << index_size_ << std::endl;
    std::cout << "Average conflicts per node\t" << node_conflicts_ / (num_model_nodes_ + num_dense_nodes_) << std::endl;
    std::cout << std::string(45, '#') << std::endl;
  }
};

struct RunStat {
  // # num_data
  uint32_t num_data_ = 0;
  // # requests
  uint32_t num_queries_ = 0;
  uint32_t num_inserts_ = 0;
  uint32_t num_updates_ = 0;
  uint32_t num_query_depth_ = 0;
  uint32_t num_query_model_ = 0;
  uint32_t num_query_bucket_ = 0;
  uint32_t num_query_dense_ = 0;
  uint32_t num_update_model_ = 0;
  uint32_t num_update_bucket_ = 0;
  uint32_t num_update_dense_ = 0;
  uint32_t num_insert_model_ = 0;
  uint32_t num_insert_bucket_ = 0;
  uint32_t num_insert_dense_ = 0;
  uint32_t num_predictions_ = 0;
  uint32_t num_comparisons_ = 0;
  // # internal operations
  uint32_t num_rebuilds_ = 0;
  uint32_t num_data_rebuild_ = 0;
  // Internal statistics
  uint32_t bucket_threshold_ = 0;
  // Space statistics
  uint32_t allocated_size_ = 0;
  // Deprecated
  uint32_t num_cont_conflicts_ = 0;
  uint32_t sum_cont_conflicts_ = 0;
  uint32_t sum_len_cont_conflicts_ = 0;
  uint32_t max_len_cont_conflicts_ = 0;

  uint32_t num_requests() {
    return num_queries_ + num_inserts_ + num_updates_;
  }

  void show() {
    std::cout << std::string(10, '#') << "Running Statistics" 
              << std::string(10, '#') << std::endl;
    std::cout << "Maximum Bucket Size\t" << bucket_threshold_ << std::endl;
    std::cout << "Number of Requests\t" << num_requests() << std::endl;
    std::cout << "Number of Queries\t" << num_queries_ 
              << "\tModel [" << num_query_model_ 
              << "] Bucket [" << num_query_bucket_ 
              << "] Dense [" << num_query_dense_ << "]\nAverage Depth\t" 
              << num_query_depth_ * 1. / num_queries_ << std::endl;
    std::cout << "Number of Updates\t" << num_updates_ 
              << "\tModel [" << num_update_model_ 
              << "] Bucket [" << num_update_bucket_ 
              << "] Dense [" << num_update_dense_ << "]" << std::endl;
    std::cout << "Number of Inserts\t" << num_inserts_ 
              << "\tModel [" << num_insert_model_ 
              << "] Bucket [" << num_insert_bucket_ 
              << "] Dense [" << num_insert_dense_ << "]" << std::endl;
    std::cout << "Number of Predictions\t" << num_predictions_ << std::endl;
    std::cout << "Number of Comparisons\t" << num_comparisons_ << std::endl;
    std::cout << "Number of Rebuilding Times\t" << num_rebuilds_ << std::endl;
    std::cout << "Number of Data in Rebuilding\t" << num_data_rebuild_ 
              << std::endl;
    std::cout << std::string(38, '#') << std::endl;
  }
};

struct ExperimentalResults {
  uint32_t batch_size;
  double bulk_load_trans_time = 0;
  double bulk_load_index_time = 0;
  double sum_transform_time = 0;
  double sum_indexing_time = 0;
  uint32_t num_requests = 0;
  uint64_t model_size = 0;
  uint64_t index_size = 0;
  std::vector<std::pair<double, double>> latencies;
  std::vector<bool> need_compute;
  uint32_t step_count = 0;

  const uint32_t kNumIncrementalReqs = 10000000;

  explicit ExperimentalResults(uint32_t b) : batch_size(b) { }

  void step() {
    if (num_requests / kNumIncrementalReqs > step_count) {
      step_count ++;
      need_compute.push_back(true);
    } else {
      need_compute.push_back(false);
    }
  }

  void show_incremental_throughputs() {
    ASSERT_WITH_MSG(latencies.size() == need_compute.size(), "Internal Error");
    double sum_latency = 0;
    uint32_t num_ops = 0;
    for (uint32_t i = 0; i < latencies.size(); ++ i) {
      sum_latency += latencies[i].first + latencies[i].second;
      num_ops += batch_size;
      if (need_compute[i] || i == latencies.size() - 1) {
        std::cout << num_ops << "\t" << num_ops * 1e3 / sum_latency << std::endl;
      }
    }
  }

  void show(bool pretty=false) {
    if (num_requests == 0) {
      sum_indexing_time = 0;
    }
    std::sort(latencies.begin(), latencies.end(), [](auto const& a, auto const& b) {
      return a.first + a.second < b.first + b.second;
    });
    std::vector<double> tail_percent = {0.5, 0.75, 0.99, 0.995, 0.9999, 1};
    double sum_time = sum_transform_time + sum_indexing_time;
    uint32_t num_ops = num_requests;
    if (pretty) {
      std::cout << std::string(10, '#') << "Experimental Results" 
                << std::string(10, '#') << std::endl;
      std::cout << std::fixed << std::setprecision(6) 
                << "BulkLoad Transformation Time\t" << bulk_load_trans_time / 1e9 << " (s)" << std::endl
                << "BulkLoad Time\t" << bulk_load_index_time / 1e9 << " (s)" << std::endl
                << "Model Size\t"<< model_size << " (bytes)" << std::endl 
                << "Index Size\t" << index_size << " (bytes)" << std::endl
                << "Throughput (Overall)\t" << num_ops * 1e3 / sum_time << " (million ops/sec)" << std::endl 
                << "Average Transform Latency\t" << sum_transform_time / num_ops << " (ns)" << std::endl
                << "Average Indexing Latency\t" << sum_indexing_time / num_ops << " (ns)" << std::endl;
      for (uint32_t i = 0; i < tail_percent.size(); ++ i) {
        uint32_t idx = std::max(0, static_cast<int>(latencies.size() * tail_percent[i]) - 1);
        std::pair<double, double> tail_latency = latencies[idx];
        std::cout << "Tail Latency (P" << tail_percent[i] * 100 <<")\t" 
                  << tail_latency.first / batch_size << " (ns)\t" 
                  << tail_latency.second / batch_size << " (ns)" << std::endl;
      }
      std::cout << std::string(40, '#') << std::endl;
    } else {
      std::cout << std::fixed << std::setprecision(6) 
                << bulk_load_trans_time / 1e9 << "\t"
                << bulk_load_index_time / 1e9 << "\t" 
                << model_size << "\t" 
                << index_size << "\t"
                << num_ops * 1e3 / sum_time << std::endl
                << sum_transform_time / num_ops << "\t"
                << sum_indexing_time / num_ops << std::endl;
      for (uint32_t i = 0; i < tail_percent.size(); ++ i) {
        uint32_t idx = std::max(0, static_cast<int>(latencies.size() * tail_percent[i]) - 1);
        std::pair<double, double> tail_latency = latencies[idx];
        std::cout << tail_latency.first / batch_size  << "\t" << tail_latency.second / batch_size << std::endl;
      }
    }
  }
};
