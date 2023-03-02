#include "core/nfl_para_impl.h"
#include "core/common.h"
#include "util/workload.h"

typedef std::chrono::time_point<std::chrono::high_resolution_clock> TT;

namespace po = boost::program_options;
using namespace aflipara;

volatile bool running = false;
std::atomic<size_t> ready_threads(0);

uint32_t num_workers = 1;
uint32_t num_bg = 1;

template<typename KT, typename VT>
struct ThreadParam {
  uint32_t id;
  NFLPara<KT, VT>* index;
  std::vector<Request<KT>>* reqs;
  uint32_t num_done_reqs;
  TT start_time;
  TT end_time;
};

template<typename KT, typename VT>
void* run_requests(void* param) {
  ThreadParam<KT, VT>& thread_param = *((ThreadParam<KT, VT>*)param);
  uint32_t thread_id = thread_param.id;
  NFLPara<KT, VT>* nfl = thread_param.index;
  std::vector<Request<KT>>& reqs = *(thread_param.reqs);

  uint32_t reqs_per_thread = std::ceil(reqs.size() / num_workers);
  uint32_t start_idx = thread_id * reqs_per_thread;
  uint32_t end_idx = (thread_id + 1) * reqs_per_thread;

  ready_threads ++;
  while (!running) ;

  VT dummy_value = 2022;
  thread_param.start_time = TIME_LOG;
  for (uint32_t i = start_idx; i < end_idx && i < reqs.size(); ++ i) {
    Request<KT>& req = reqs[i];
    if (req.op == kQuery) {
      VT value;
      // COUT_INFO_W_LOCK("Thread " << thread_id << " query " << req.key);
      bool found = nfl->find(req.key, value);
    } else if (req.op == kInsert) {
      // COUT_INFO_W_LOCK("Thread " << thread_id << " insert " << req.key);
      nfl->insert({req.key, dummy_value});
    }
    thread_param.num_done_reqs ++;
  }
  thread_param.end_time = TIME_LOG;
  ready_threads --;
  pthread_exit(nullptr);
}

template<typename KT, typename VT>
void test_workload(std::string workload_path) {
  std::vector<KT> init_keys;
  std::vector<std::pair<KT, VT>> init_kvs;
  std::vector<Request<KT>> reqs;
  load_workload(workload_path, init_keys, reqs);
  init_kvs.reserve(init_keys.size());
  for (uint32_t i = 0; i < init_keys.size(); ++ i) {
    init_kvs.push_back({init_keys[i], i});
  }
  COUT_INFO("# loading data [" << init_kvs.size() << "]")
  COUT_INFO("# requests [" << reqs.size() << "]")

  auto bulk_load_start = TIME_LOG;
  NFLPara<KT, VT> nfl(num_bg);
  auto bulk_load_mid = TIME_LOG;
  nfl.bulk_load(init_kvs.data(), init_kvs.size());
  // nfl.print_statistics();
  auto bulk_load_end = TIME_LOG;
  double construct_index_time = TIME_IN_SECOND(bulk_load_start, bulk_load_mid);
  double bulk_load_index_time = TIME_IN_SECOND(bulk_load_mid, bulk_load_end);

  COUT_INFO("Bulk loading\t" << construct_index_time << " sec\t"
            << bulk_load_index_time << " sec")
  COUT_INFO("# Nodes\t" << nfl.hyper_para.num_nodes)

  pthread_t threads[num_workers];
  ThreadParam<KT, VT> thread_params[num_workers];

  running = false;
  for (uint32_t i = 0; i < num_workers; ++ i) {
    thread_params[i].id = i;
    thread_params[i].index = &nfl;
    thread_params[i].reqs = &reqs;
    thread_params[i].num_done_reqs = 0;
    int ret = pthread_create(&threads[i], nullptr, run_requests<KT, VT>, 
                             (void *)&thread_params[i]);
    ASSERT_WITH_MSG(!ret, "Error Code\t" << ret << " of Thread-" << i)
  }

  while (ready_threads < num_workers) {
    sleep(1);
  }

  std::vector<size_t> tput_history(num_workers, 0);
  running = true;
  while (ready_threads > 0) {
    sleep(1);
    uint32_t tot_num_reqs = 0;
    for (uint32_t i = 0; i < num_workers; ++ i) {
      tot_num_reqs += thread_params[i].num_done_reqs - tput_history[i];
      tput_history[i] = thread_params[i].num_done_reqs;
    }
    COUT_INFO("Throughput: " << tot_num_reqs / 1e6 << " million ops/sec, " 
              << "latency: " << 1e9 / tot_num_reqs << " ns")
  }
  running = false;
  
  for (uint32_t i = 0; i < num_workers; ++ i) {
    int rc = pthread_join(threads[i], nullptr);
    ASSERT_WITH_MSG(!rc, "Return Code\t" << rc << " of Thread-" << i)
  }
  double sum_latency = 0;
  for (uint32_t i = 0; i < num_workers; ++ i) {
    double latency = TIME_IN_NANO_SECOND(thread_params[i].start_time, 
                                         thread_params[i].end_time);
    sum_latency = std::max(sum_latency, latency);
  }
  COUT_INFO("Overall Throughput: " << reqs.size() * 1e3 / sum_latency
            << " million ops/sec, average latency: " 
            << sum_latency / reqs.size() << " ns")
}

template<typename KT, typename VT>
void test_raw_dataset(std::string data_path) {
  std::vector<KT> keys;
  load_keyset(data_path, keys);
  uint32_t num_keys = keys.size();
  NFLPara<KT, VT> nfl(num_bg);
  std::vector<uint32_t> idx;
  for (uint32_t i = 0; i < num_keys; ++ i) {
    idx.push_back(i);
  }
  shuffle(idx, 0, num_keys);
  std::vector<std::pair<KT, VT>> init_data;
  std::vector<std::pair<KT, VT>> insert_data;
  init_data.reserve(num_keys / 2);
  insert_data.reserve(num_keys - num_keys / 2);
  for (uint32_t i = 0; i < num_keys; ++ i) {
    if (i < num_keys / 2) {
      init_data.push_back({keys[idx[i]], i});
    } else {
      insert_data.push_back({keys[idx[i]], i});
    }
  }
  std::sort(init_data.begin(), init_data.end(), 
    [](auto const& a, auto const& b) {
      return a.first < b.first;
  });
  // Test bulk loading
  COUT_INFO("Test bulk loading, number of keys [" << init_data.size() << "]")
  nfl.bulk_load(init_data.data(), init_data.size());
  nfl.print_statistics();

  // Test query
  COUT_INFO("Test Querying")
  for (uint32_t i = 0; i < init_data.size(); ++ i) {
    VT value;
    bool found = nfl.find(init_data[i].first, value);
    ASSERT_WITH_MSG(found, "Cannot find " << i << "th key (" 
                    << init_data[i].first << ")")
  }
  // Test insert
  COUT_INFO("Test Insertion")
  for (uint32_t i = 0; i < insert_data.size(); ++ i) {
    nfl.insert(insert_data[i]);
  }
  nfl.print_statistics();
  // Test query
  COUT_INFO("Test Querying After Insertion")
  for (uint32_t i = 0; i < init_data.size(); ++ i) {
    VT value;
    bool found = nfl.find(init_data[i].first, value);
    ASSERT_WITH_MSG(found, "Cannot find " << i << "th loading key (" 
                    << init_data[i].first << ")")
  }
  for (uint32_t i = 0; i < insert_data.size(); ++ i) {
    VT value;
    bool found = nfl.find(insert_data[i].first, value);
    ASSERT_WITH_MSG(found, "Cannot find " << i << "th inserted key (" 
                    << insert_data[i].first << ")")
  }
  COUT_INFO("Test Success")
}

template<typename KT, typename VT>
void test_synthetic(uint32_t num_data) {
  std::vector<std::pair<KT, VT>> init_data;
  std::vector<std::pair<KT, VT>> ins_data;
  uint32_t num_init_data = num_data / 2;
  uint32_t num_ins_data = num_data - num_init_data;
  init_data.reserve(num_init_data);
  ins_data.reserve(num_ins_data);
  
  std::vector<uint32_t> idx;
  idx.reserve(num_data);
  for (uint32_t i = 0; i < num_data; ++ i) {
    idx.push_back(i);
  }
  shuffle(idx, 0, idx.size());
  for (uint32_t i = 0; i < num_init_data; ++ i) {
    init_data.push_back({idx[i], idx[i]});
  }
  for (uint32_t i = num_init_data; i < num_data; ++ i) {
    ins_data.push_back({idx[i], idx[i]});
  }
  std::sort(init_data.begin(), init_data.end(), 
    [](const auto& a, const auto& b) {
      return a.first < b.first;
  });

  NFLPara<KT, VT> nfl(num_bg);
  nfl.bulk_load(init_data.data(), init_data.size());
  
  for (uint32_t i = 0; i < init_data.size(); ++ i) {
    VT val = 0;
    nfl.find(init_data[i].first, val);
    ASSERT_WITH_MSG(val == init_data[i].second, "Find {" << init_data[i].first 
                    << ", " << init_data[i].second << "}, but got {" << val 
                    << "}")
  }
  
  for (uint32_t i = 0; i < ins_data.size(); ++ i) {
    nfl.insert(ins_data[i]);
  }
  
  for (uint32_t i = 0; i < ins_data.size(); ++ i) {
    VT val = 0;
    nfl.find(ins_data[i].first, val);
    ASSERT_WITH_MSG(val == ins_data[i].second, "Find {" << ins_data[i].first 
                    << ", " << ins_data[i].second << "}, but got {" << val 
                    << "}")
  }

  for (uint32_t i = 0; i < init_data.size(); ++ i) {
    VT val = 0;
    nfl.find(init_data[i].first, val);
    ASSERT_WITH_MSG(val == init_data[i].second, "Find {" << init_data[i].first 
                    << ", " << init_data[i].second << "}, but got {" << val 
                    << "}")
  }
  COUT_INFO("Success")
}

int main(int argc, char* argv[]) {
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", (tostr("example: ./test_afli_para ") 
     + "--data_path books-200M-20R-zipf.bin " 
     + "--test_type workload --num_workers 1 --num_bg 2").data())
    ("data_path", po::value<std::string>(), 
     "the path of data")
    ("test_type", po::value<std::string>(), 
     "the test type")
    ("key_type", po::value<std::string>(), 
     "the key type of workload, e.g., double, int32, int64")
    ("value_type", po::value<std::string>(), 
     "the value type of workload, e.g., double, int32, int64")
    ("num_workers", po::value<uint32_t>(), 
     "the number of user threads")
    ("num_bg", po::value<uint32_t>(), 
     "the number of background threads")
    ("num_data", po::value<uint32_t>(), 
     "the number of synthetic data")
  ;

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
  } catch (...) {
    COUT_ERR("Unrecognized parameters, please use --help");
  }
  po::notify(vm);

  if (vm.count("help")) {
    COUT_INFO(desc)
    return 0;
  }

  check_options(vm, {"test_type"});
  std::string test_type = vm["test_type"].as<std::string>();
  if (test_type == "raw" || test_type == "workload") {
    check_options(vm, {"data_path", "key_type", "value_type", "num_workers", 
                  "num_bg"});
  } else if (test_type == "synthetic") {
    check_options(vm, {"num_data", "key_type", "value_type", "num_workers", 
                  "num_bg"});
  }
  std::string key_type = vm["key_type"].as<std::string>();
  std::string value_type = vm["value_type"].as<std::string>();
  num_workers = vm["num_workers"].as<uint32_t>();
  num_bg = vm["num_bg"].as<uint32_t>();
  COUT_INFO("# user threads: " << num_workers << "\t# bg threads: " << num_bg)
  if (test_type == "raw") {
    std::string data_path = vm["data_path"].as<std::string>();
    if (key_type == "double" && value_type == "uint64") {
      test_raw_dataset<double, uint64_t>(data_path);
    } else if (key_type == "int64" && value_type == "uint64") {
      test_raw_dataset<int64_t, uint64_t>(data_path);
    } else if (key_type == "uint64" && value_type == "uint64") {
      test_raw_dataset<uint64_t, uint64_t>(data_path);
    } else {
      COUT_ERR("Unsupported key type [" << key_type << "] value type [" 
               << value_type << "]")
    }
  } else if (test_type == "workload") {
    std::string data_path = vm["data_path"].as<std::string>();
    if (key_type == "double" && value_type == "uint64") {
      test_workload<double, uint64_t>(data_path);
    } else if (key_type == "int64" && value_type == "uint64") {
      test_workload<int64_t, uint64_t>(data_path);
    } else if (key_type == "uint64" && value_type == "uint64") {
      test_workload<uint64_t, uint64_t>(data_path);
    } else {
      COUT_ERR("Unsupported key type [" << key_type << "] value type [" 
               << value_type << "]")
    }
  } else if (test_type == "synthetic") {
    uint32_t num_data = vm["num_data"].as<uint32_t>();
    if (key_type == "double" && value_type == "uint64") {
      test_synthetic<double, uint64_t>(num_data);
    } else if (key_type == "int64" && value_type == "uint64") {
      test_synthetic<int64_t, uint64_t>(num_data);
    } else if (key_type == "uint64" && value_type == "uint64") {
      test_synthetic<uint64_t, uint64_t>(num_data);
    } else {
      COUT_ERR("Unsupported key type [" << key_type << "] value type [" 
               << value_type << "]")
    }
  } else {
    COUT_ERR("Unsupported test type\t" << test_type)
  }
  return 0;
}