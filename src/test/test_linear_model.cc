#include "core/common.h"
#include "core/conflicts.h"
#include "util/workload.h"

namespace po = boost::program_options;
using namespace aflipara;

void load_keys(std::string key_path, std::string key_type, 
               std::vector<long double>& keys) {
  if (key_type == "double") {
    std::vector<double> origin_keys;
    load_keyset(key_path, origin_keys);
    keys.reserve(origin_keys.size());
    for (uint32_t i = 0; i < origin_keys.size(); ++ i) {
      keys.push_back(origin_keys[i]);
    }
  } else if (key_type == "uint64") {
    std::vector<uint64_t> origin_keys;
    load_keyset(key_path, origin_keys);
    keys.reserve(origin_keys.size());
    for (uint32_t i = 0; i < origin_keys.size(); ++ i) {
      keys.push_back(origin_keys[i]);
    }
  } else if (key_type == "int64") {
    std::vector<int64_t> origin_keys;
    load_keyset(key_path, origin_keys);
    keys.reserve(origin_keys.size());
    for (uint32_t i = 0; i < origin_keys.size(); ++ i) {
      keys.push_back(origin_keys[i]);
    }
  }
}

void collect_conflicts(const long double* keys, uint32_t size, double size_amp,
                       std::vector<uint32_t>& conflicts) {
  LinearModel model;
  LinearModelBuilder builder;
  for (uint32_t i = 0; i < size; ++ i) {
    builder.add(keys[i], i);
  }
  builder.build(&model);
  double min_key = keys[0];
  double max_key = keys[size - 1];
  model.intercept = -model.slope * min_key + 0.5;
  int64_t capacity = static_cast<int64_t>(size * size_amp);
  int64_t predicted_size = model.predict(max_key) + 1;
  if (predicted_size > 1) {
    capacity = std::min(predicted_size, capacity);
  }

  uint32_t conflict = 1;
  int64_t last_pos = std::min(std::max(model.predict(min_key), 0L), 
                                       capacity - 1);
  for (uint32_t i = 1; i < size; ++ i) {
    int64_t pos = std::min(std::max(model.predict(keys[i]), 0L), capacity - 1);
    if (pos == last_pos) {
      conflict ++;
    } else {
      conflicts.push_back(conflict);
      last_pos = pos;
      conflict = 1;
    }
  }
  if (conflict > 0) {
    conflicts.push_back(conflict);
  }
}

int main(int argc, char* argv[]) {
  po::options_description desc("Allowed options");
  desc.add_options()
    ("test_type", po::value<std::string>(), 
     "the test type, e.g., keyset, synthetic")
    ("key_path", po::value<std::string>(), 
     "the path of keys")
    ("key_type", po::value<std::string>(), 
     "the key type, e.g., double, int32, int64")
    ("key_range", po::value<long double>(), 
     "the key range")
    ("size_amp", po::value<double>(), 
     "the size amplification")
    ("tail_ratio", po::value<double>(), 
     "the tail ratio")
  ;

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
  } catch (...) {
    COUT_ERR("Unrecognized parameters, please use --help");
  }
  po::notify(vm);

  check_options(vm, {"test_type", "key_range"});
  std::string test_type = vm["test_type"].as<std::string>();
  long double key_range = vm["key_range"].as<long double>();
  double size_amp = 1.5;
  if (vm.count("size_amp")) {
    size_amp = vm["size_amp"].as<double>();
  }
  double tail_ratio = 0.99;
  if (vm.count("tail_ratio")) {
    tail_ratio = vm["tail_ratio"].as<double>();
  }

  std::vector<long double> keys;
  std::vector<uint32_t> conflicts;
  if (test_type == "synthetic") {

  } else if (test_type == "keyset") {
    check_options(vm, {"key_path", "key_type"});
    std::string key_path = vm["key_path"].as<std::string>();
    std::string key_type = vm["key_type"].as<std::string>();
    load_keys(key_path, key_type, keys);
    std::sort(keys.begin(), keys.end());

    long double key_space = (*(keys.rbegin())) - (*(keys.begin()));
    uint32_t num_seg = std::ceil(key_space / key_range);
    COUT_INFO("# Keys [" << keys.size() << "], key space [" << key_space 
              << "], min key [" << (*(keys.begin())) << "], max key [" 
              << (*(keys.rbegin())) << "]");
    COUT_INFO("Range [" << key_range << "], # segments [" << num_seg << "]");
    uint32_t l = 0;
    long double start_key = keys[0];
    long double end_key = start_key + key_range;
    for (uint32_t i = 0; i < num_seg; ++ i) {
      uint32_t r = l;
      while (r < keys.size() && keys[r] < end_key) {
        r ++;
      }
      if (l < r) {
        collect_conflicts(keys.data() + l, r - l, size_amp, conflicts);
      }
      l = r;
    }
  }
  std::sort(conflicts.begin(), conflicts.end());
  int32_t tail_idx = int32_t(conflicts.size() * tail_ratio) - 1;
  COUT_INFO("Max conflict [" << (*(conflicts.rbegin())) 
            << "], tail conflicts [" << conflicts[tail_idx] << "]");
  return 0;
}