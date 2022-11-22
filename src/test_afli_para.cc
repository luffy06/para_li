#include "afli_para/afli_para_impl.h"
#include "util/common.h"
#include "util/workload.h"

typedef double KT;
typedef long long int VT;
typedef std::pair<KT, VT> KVT;
typedef std::chrono::time_point<std::chrono::high_resolution_clock> TT;

using namespace aflipara;

volatile bool running = false;
std::atomic<size_t> ready_threads(0);

uint32_t num_threads = 1;
uint32_t num_bg = 1;

template<typename KT, typename VT>
struct ThreadParam {
  uint32_t id;
  AFLIPara<KT, VT>* index;
  std::vector<Request<KT, VT>>* reqs;
  TT start_time;
  TT end_time;
};

template<typename KT, typename VT>
void* run_requests(void* param) {
  ThreadParam<KT, VT>& thread_param = *((ThreadParam<KT, VT>*)param);
  uint32_t thread_id = thread_param.id;
  AFLIPara<KT, VT>* afli = thread_param.index;
  std::vector<Request<KT, VT>>& reqs = *(thread_param.reqs);

  uint32_t reqs_per_thread = std::ceil(reqs.size() / num_threads);
  uint32_t start_idx = thread_id * reqs_per_thread;
  uint32_t end_idx = (thread_id + 1) * reqs_per_thread;

  ready_threads ++;
  while (!running) ;

  thread_param.start_time = std::chrono::high_resolution_clock::now();
  for (uint32_t i = start_idx; i < end_idx && i < reqs.size(); ++ i) {
    Request<KT, VT>& req = reqs[i];
    bool print = false;
    if ((i - start_idx) % 40000 == 0) {
      print = true;
    }
    if (req.op == kQuery) {
      // if (print) {
        COUT_W_LOCK("\nProgress Read " << i - start_idx << "/" << end_idx - start_idx)
      // }
      VT value;
      bool found = afli->find(req.kv.first, value);
    } else if (req.op == kInsert) {
      // if (print) {
        COUT_W_LOCK("\nProgress Insert " << i - start_idx << "/" << end_idx - start_idx)
      // }
      afli->insert(req.kv);
    }
    if (print) {
      auto end_time = std::chrono::high_resolution_clock::now();
      double sum_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    end_time - thread_param.start_time).count();
      std::cout << "Throughput\t" << (i - start_idx) * 1000. / (sum_latency) 
          << " million ops/sec" << std::endl;
    }
  }
  thread_param.end_time = std::chrono::high_resolution_clock::now();
  ready_threads --;
  pthread_exit(nullptr);
}

void test_workload(std::string workload_path) {
  std::vector<std::pair<KT, VT>> init_data;
  std::vector<Request<KT, VT>> reqs;
  load_workloads(workload_path, init_data, reqs);
  auto bulk_load_start = std::chrono::high_resolution_clock::now();
  AFLIPara<KT, VT> afli(num_bg);
  auto bulk_load_mid = std::chrono::high_resolution_clock::now();
  afli.bulk_load(init_data.data(), init_data.size());
  // afli.print_statistics();
  auto bulk_load_end = std::chrono::high_resolution_clock::now();
  double construct_index_time = 
    std::chrono::duration_cast<std::chrono::nanoseconds>(bulk_load_mid 
                                                  - bulk_load_start).count();
  double bulk_load_index_time = 
    std::chrono::duration_cast<std::chrono::nanoseconds>(bulk_load_end 
                                                  - bulk_load_mid).count();

  std::cout << "# REQ\t" << reqs.size() << std::endl;
  std::cout << std::fixed << std::setprecision(6) << "Bulk loading\t" 
            << construct_index_time / 1e9 << " sec\t"
            << bulk_load_index_time / 1e9 << " sec" << std::endl;
  std::cout << "# Nodes\t" << afli.hyper_para.num_nodes << std::endl;

  // reqs.resize(400000);
  pthread_t threads[num_threads];
  ThreadParam<KT, VT> thread_params[num_threads];

  running = false;
  for (uint32_t i = 0; i < num_threads; ++ i) {
    thread_params[i].id = i;
    thread_params[i].index = &afli;
    thread_params[i].reqs = &reqs;
    int ret = pthread_create(&threads[i], nullptr, run_requests<KT, VT>, 
                              (void *)&thread_params[i]);
    if (ret) {
      std::cout << "Error Code\t" << ret << " of Thread-" << i << std::endl;
      exit(-1);
    }
  }

  while (ready_threads < num_threads) {
    sleep(1);
  }

  uint32_t interval = 10;
  std::vector<size_t> tput_history(num_threads, 0);
  running = true;
  while (ready_threads > 0) {
    // std::this_thread::sleep_for(std::chrono::milliseconds(interval));
    // sum_latency += interval;
    // uint32_t tot_num_reqs = 0;
    // for (uint32_t i = 0; i < num_threads; ++ i) {
    //   tot_num_reqs += thread_params[i].num_reqs - tput_history[i];
    //   tput_history[i] = thread_params[i].num_reqs;
    // }
    // std::cout << "Throughput\t" << tot_num_reqs / (1000. * interval) << " million ops/sec" << std::endl;
    // std::cout << tot_num_reqs / (1000. * interval) << std::endl;
  }
  running = false;
  
  for (uint32_t i = 0; i < num_threads; ++ i) {
    int rc = pthread_join(threads[i], nullptr);
    if (rc) {
      std::cout << "Return Code\t" << rc << " of Thread-" << i << std::endl;
      exit(-1);
    }
  }
  TT start_time = thread_params[0].start_time;
  TT end_time = thread_params[0].end_time;
  for (uint32_t i = 0; i < num_threads; ++ i) {
    start_time = std::min(start_time, thread_params[i].start_time);
    end_time = std::max(end_time, thread_params[i].end_time);
  }
  double sum_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        end_time - start_time).count();
  std::cout << "Overall Throughput\t" << reqs.size() * 1000. / (sum_latency) 
            << " million ops/sec" << std::endl;
}

void test_raw_dataset(std::string data_path) {
  std::vector<std::pair<KT, VT>> kvs;
  load_source_data(data_path, kvs);
  uint32_t num_keys = kvs.size();
  AFLIPara<KT, VT> afli(num_bg);
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
      init_data.push_back(kvs[idx[i]]);
    } else {
      insert_data.push_back(kvs[idx[i]]);
    }
  }
  std::sort(init_data.begin(), init_data.end(), 
    [](auto const& a, auto const& b) {
      return a.first < b.first;
  });
  // DEBUG_KEY = init_data[353870].first;
  // Test bulk loading
  std::cout << "Test Bulk Loading, Number of Keys\t" 
            << init_data.size() << std::endl;
  afli.bulk_load(init_data.data(), init_data.size());
  afli.print_statistics();

  // Test query
  std::cout << "Test Querying" << std::endl;
  for (int i = 0; i < init_data.size(); ++ i) {
    VT value;
    bool found = afli.find(init_data[i].first, value);
    if (!found) {
      std::cout << std::fixed << std::setprecision(6) 
                << "Cannot find " << i << "th key (" 
                << init_data[i].first << ")" << std::endl;
      exit(-1);
    }
  }
  // Test insert
  std::cout << "Test Insertion" << std::endl;
  for (int i = 0; i < insert_data.size(); ++ i) {
    afli.insert(insert_data[i]);
  }
  afli.print_statistics();
  // Test query
  std::cout << "Test Querying After Insertion" << std::endl;
  for (int i = 0; i < init_data.size(); ++ i) {
    VT value;
    bool found = afli.find(init_data[i].first, value);
    if (!found) {
      std::cout << std::fixed << std::setprecision(6) 
                << "Cannot find " << i << "th loading key (" 
                << init_data[i].first << ")" << std::endl;
      exit(-1);
    }
  }
  for (int i = 0; i < insert_data.size(); ++ i) {
    VT value;
    bool found = afli.find(insert_data[i].first, value);
    if (!found) {
      std::cout << std::fixed << std::setprecision(6) 
                << "Cannot find " << i << "th inserted key (" 
                << insert_data[i].first << ")" << std::endl;
      exit(-1);
    }
  }
  std::cout << "Test Success" << std::endl;  
}

int main(int argc, char* argv[]) {
  if (argc < 5) {
    std::cout << "No enough parameters" << std::endl;
    std::cout << "Please input: test_index (data path) (test type) " 
              << "(number of threads) (number of bg threads)" << std::endl;
    exit(-1);
  }
  std::string data_path = std::string(argv[1]);
  std::string test_type = std::string(argv[2]);
  num_threads = std::strtoul(argv[3], nullptr, 10);
  num_bg = std::strtoul(argv[4], nullptr, 10);
  std::cout << "# threads\t" << num_threads << std::endl;
  if (test_type == "raw") {
    test_raw_dataset(data_path);
  } else if (test_type == "workload") {
    test_workload(data_path);
  } else {
    std::cout << "Unsupported test type\t" << test_type << std::endl;
    exit(-1);
  }
  return 0;
}