#include "core/common.h"
#include "util/workload.h"

namespace po = boost::program_options;
using namespace aflipara;

template<typename KT>
void load_keyset_old(std::string path, std::vector<KT>& keys) {
  std::ifstream in(path, std::ios::binary | std::ios::in);
  if (!in.is_open()) {
    std::cout << "File [" << path << "] does not exist" << std::endl;
    exit(-1);
  }
  int num_keys = 0;
  in.read((char*)&num_keys, sizeof(int));
  keys.reserve(num_keys);
  for (int i = 0; i < num_keys; ++ i) {
    KT key;
    in.read((char*)&key, sizeof(KT));
    keys.push_back(key);
  }
  in.close();
}

void load_keys(std::string key_path, std::string key_type, 
               std::vector<long double>& keys, bool use_old=false) {
  if (key_type == "double") {
    std::vector<double> origin_keys;
    if (!use_old) {
      load_keyset(key_path, origin_keys);
    } else {
      load_keyset_old(key_path, origin_keys);
    }
    keys.reserve(origin_keys.size());
    for (uint32_t i = 0; i < origin_keys.size(); ++ i) {
      keys.push_back(origin_keys[i]);
    }
  } else if (key_type == "uint64") {
    std::vector<uint64_t> origin_keys;
    if (!use_old) {
      load_keyset(key_path, origin_keys);
    } else {
      load_keyset_old(key_path, origin_keys);
    }
    keys.reserve(origin_keys.size());
    for (uint32_t i = 0; i < origin_keys.size(); ++ i) {
      keys.push_back(origin_keys[i]);
    }
  } else if (key_type == "int64") {
    std::vector<int64_t> origin_keys;
    if (!use_old) {
      load_keyset(key_path, origin_keys);
    } else {
      load_keyset_old(key_path, origin_keys);
    }
    keys.reserve(origin_keys.size());
    for (uint32_t i = 0; i < origin_keys.size(); ++ i) {
      keys.push_back(origin_keys[i]);
    }
  }
}

void load_sosd_keys(std::string path, std::string key_type, 
                    std::vector<long double>& keys) {
  std::vector<uint64_t> sosd_keys;
  std::ifstream in(path, std::ios::binary | std::ios::in);
  ASSERT_WITH_MSG(in.is_open(), "File [" << path << "] does not exist")
  uint64_t num_keys = 0;
  in.read((char*)&num_keys, sizeof(uint64_t));
  sosd_keys.resize(num_keys);
  in.read((char*)sosd_keys.data(), num_keys * sizeof(uint64_t));
  in.close();
  keys.reserve(sosd_keys.size());
  for (uint32_t i = 0; i < sosd_keys.size(); ++ i) {
    keys.push_back(sosd_keys[i]);
  }
}

int main(int argc, char* argv[]) {
  po::options_description desc("Allowed options");
  desc.add_options()
    ("new_key_path", po::value<std::string>(), 
     "the path of source data")
    ("new_key_type", po::value<std::string>(), 
     "the key type of source data, e.g., double, int32, int64")
    ("old_key_path", po::value<std::string>(), 
     "the path of target data")
    ("old_key_type", po::value<std::string>(), 
     "the key type of target data, e.g., double, int32, int64")
    ("sosd_keys", "is sosd keys or not")
    ("num_samples", po::value<int>(), 
     "the number of samples")
  ;

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
  } catch (...) {
    COUT_ERR("Unrecognized parameters, please use --help");
  }
  po::notify(vm);

  check_options(vm, {"new_key_path", "new_key_type", "old_key_path", 
                     "old_key_type"});
  std::string new_key_path = vm["new_key_path"].as<std::string>();
  std::string new_key_type = vm["new_key_type"].as<std::string>();
  std::string old_key_path = vm["old_key_path"].as<std::string>();
  std::string old_key_type = vm["old_key_type"].as<std::string>();
  bool sosd_keys = vm.count("sosd_keys");
  int num_samples = 25;
  if (vm.count("num_samples")) {
    num_samples = vm["num_samples"].as<int>();
  }
  
  std::vector<long double> new_keys;
  std::vector<long double> old_keys;
  if (!sosd_keys) {
    load_keys(new_key_path, new_key_type, new_keys);
    load_keys(old_key_path, old_key_type, old_keys);
  } else {
    load_sosd_keys(new_key_path, new_key_type, new_keys);
    load_sosd_keys(old_key_path, old_key_type, old_keys);
  }
  std::sort(new_keys.begin(), new_keys.end());
  std::sort(old_keys.begin(), old_keys.end());
  COUT_INFO("First " << num_samples << " keys\nIndex\tSource keys\tTarget keys");
  for (int i = 0; i < num_samples; ++ i) {
    COUT_INFO(i << "\t" << new_keys[i] << "\t" << old_keys[i]);
  }
  COUT_INFO("Last " << num_samples << " keys\nIndex\tSource keys\tTarget keys");
  for (int i = num_samples; i > 0; -- i) {
    COUT_INFO(new_keys.size() - i << "\t" << new_keys[new_keys.size() - i] 
              << "\t" << old_keys[old_keys.size() - i]);
  }
  if (new_keys.size() != old_keys.size()) {
    COUT_INFO("Different size, SRC [" << new_keys.size() 
              << "], TAR [" << old_keys.size() << "]");
  } else {
    COUT_INFO("# keys\t" << new_keys.size());
  }
  uint32_t num_logs = 0;
  for (uint32_t i = 0; i < std::min(new_keys.size(), old_keys.size()); ++ i) {
    if (!equal(new_keys[i], old_keys[i])) {
      COUT_INFO("Different key, Index [" << i << "], SRC [" 
                << new_keys[i] << "], TAR [" << old_keys[i] << "]");
      num_logs ++;
      ASSERT_WITH_MSG(num_logs < 1000, "Too many logs");
    }
  }
  if (num_logs == 0) {
    COUT_INFO("Success: all keys are the same");
  }
  return 0;
}