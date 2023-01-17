#ifndef WORKLOAD_H
#define WORKLOAD_H

#include "zipf.h"
#include "common.h"

namespace aflipara {

enum OperationType {
  kBulkLoad = 0,
  kQuery = 1,
  kUpdate = 2,
  kRemove = 3,
  kInsert = 4,
  kMixed = 5
};

template<typename KT>
struct Request {
  OperationType op;
  KT key;
};

std::string get_operation(OperationType op) {
  if (op == kBulkLoad) {
    return "bulkload";
  } else if (op == kQuery) {
    return "query";
  } else if (op == kUpdate) {
    return "update";
  } else if (op == kRemove) {
    return "remove";
  } else if (op == kInsert) {
    return "insert";
  } else if (op == kMixed) {
    return "batch_operations";
  } else {
    COUT_ERR("Error operation [" << op << "]")
  }
}

template<typename KT>
void load_keyset(std::string path, std::vector<KT>& keys) {
  std::ifstream in(path, std::ios::binary | std::ios::in);
  if (!in.is_open()) {
    COUT_ERR("File [" << path << "] does not exist")
  }
  uint32_t num_keys = 0;
  in.read((char*)&num_keys, sizeof(uint32_t));
  keys.reserve(num_keys);
  for (uint32_t i = 0; i < num_keys; ++ i) {
    KT key;
    in.read((char*)&key, sizeof(KT));
    keys.push_back(key);
  }
  in.close();
}

template<typename KT>
void store_keyset(std::string path, const std::vector<KT>& keys) {
  std::fstream out(path, std::ios::out | std::ios::binary);
  if (!out.is_open()) {
    COUT_ERR("File [" << path << "] doesn't exist")
  }
  uint32_t num_keys = keys.size();
  out.write((char*)&num_keys, sizeof(uint32_t));
  for (uint32_t i = 0; i < keys.size(); ++ i) {
    out.write((char*)&keys[i], sizeof(KT));
  }
  out.close();
}

template<typename KT>
void load_workload(std::string path, std::vector<KT>& init_data, 
                   std::vector<Request<KT>>& work_reqs) {
  std::ifstream in(path, std::ios::binary | std::ios::in);
  if (!in.is_open()) {
    COUT_ERR("File [" << path << "] does not exist")
  }
  uint32_t num_reqs = 0;
  in.read((char*)&num_reqs, sizeof(uint32_t));
  init_data.reserve(num_reqs);
  work_reqs.reserve(num_reqs);
  for (uint32_t i = 0; i < num_reqs; ++ i) {
    Request<KT> req;
    in.read((char*)&req, sizeof(Request<KT>));
    if (req.op == kBulkLoad) {
      init_data.push_back(req.key);
    } else {
      work_reqs.push_back(req);
    }
  }
  in.close();
}

template<typename KT>
void store_workload(std::string path, 
                    const std::vector<Request<KT>>& work_reqs) {
  std::ofstream out(path, std::ios::binary | std::ios::out);
  if (!out.is_open()) {
    COUT_ERR("File [" << path << "] does not exist")
  }
  uint32_t num_reqs = work_reqs.size();
  out.write((char*)&num_reqs, sizeof(int));
  for (int i = 0; i < work_reqs.size(); ++ i) {
    out.write((char*)&work_reqs[i], sizeof(Request<KT>));
  }
  out.close();
}

template<typename KT>
void check_workload(const std::vector<Request<KT>>& reqs) {
  std::set<KT> unique_keys;
  for (uint32_t i = 0; i < reqs.size(); ++ i) {
    const Request<KT>& req = reqs[i];
    if (req.op == kBulkLoad || req.op == kInsert) {
      if (unique_keys.find(req.key) != unique_keys.end()) {
        COUT_ERR("Non-unique keys in the generated workload")
      } else {
        unique_keys.insert(req.key);
      }
    } else if (req.op == kQuery || req.op == kUpdate || req.op == kRemove) {
      if (unique_keys.find(req.key) == unique_keys.end()) {
        COUT_ERR("Cannot find the requested key")
      }
    } else {
      COUT_ERR("Unsupported operation")
    }
  }
}

template<class DType, typename KT>
void generate_synthetic_keyset(DType dist, uint32_t num_keys, 
                               std::vector<KT>& keys, std::string path="", 
                               const double kScaleUp=1e9) {
  ASSERT_WITH_MSG(num_keys > 0, "Key numbers [" + tostr(num_keys) + "] is wrong");
  std::mt19937_64 gen(kSeed);
  std::set<KT> key_set;
  for (uint32_t i = 0; i < num_keys * 5 && key_set.size() < num_keys; ++ i) {
    KT key = static_cast<KT>(dist(gen) * kScaleUp + 0.5);
    key_set.insert(key);
  }
  keys.reserve(num_keys);
  for (auto key : key_set) {
    keys.push_back(key);
  }
  COUT_INFO("Info: [" << keys.size() << "] unique keys are generated")
  if (path != "") {
    store_keyset(path, keys);
  }
}

template<typename KT>
void generate_offline_workload(std::string workload_path, std::string key_path, 
                               double init_frac, std::string req_dist, 
                               uint32_t num_reqs, double query_frac, 
                               double update_frac, double remove_frac, 
                               double insert_frac, double kks_insert_frac=1) {
  ASSERT_WITH_MSG(equal(query_frac + update_frac + remove_frac + insert_frac, 1.), 
           "The sum of the read, update, remove, insert proportion is not 1");
  ASSERT_WITH_MSG(kks_insert_frac >= 0 && kks_insert_frac <= 1, 
           "The known-key-space insert proportion is illegal");
  
  // Load keyset
  std::vector<KT> keys;
  load_keyset(key_path, keys);
  
  // Ensure that all keys are sorted
  std::sort(keys.begin(), keys.end(), 
    [](auto const& a, auto const& b) {
      return a < b;
  });

  // Verify unique keys
  for (uint32_t i = 1; i < keys.size(); ++ i) {
    if (equal(keys[i], keys[i - 1])) {
      COUT_ERR("Duplicated keys, " << i - 1 << "th key [" << keys[i - 1] 
               << "], " << i << "th key [" << keys[i] << "]")
    }
  }

  // Generate the read-write workloads
  uint32_t num_keys = keys.size();
  uint32_t num_init = std::round(num_keys * init_frac);
  uint32_t num_query_keys = std::round(num_reqs * query_frac);
  uint32_t num_update_keys = std::round(num_reqs * update_frac);
  uint32_t num_remove_keys = std::round(num_reqs * remove_frac);
  uint32_t num_insert_keys = std::min(uint32_t(std::round(num_reqs 
                                      * insert_frac)), num_keys - num_init);
  uint32_t num_kks_insert = std::round(num_insert_keys * kks_insert_frac);
  uint32_t num_oob_insert = num_insert_keys - num_kks_insert;

  COUT_INFO("Total number of keys [" << num_keys << "]\n" 
            << "Initial number of keys [" << num_init << "]\n" 
            << "Total number of requests [" << num_reqs << "]\n" 
            << "\tNumber of queries [" << num_query_keys << "]\n" 
            << "\tNumber of updates [" << num_update_keys << "]\n" 
            << "\tNumber of removes [" << num_remove_keys << "]\n" 
            << "\tNumber of inserts [" << num_insert_keys << "]\n" 
            << "\t\tNumber of known-key-space inserts [" << num_kks_insert 
            << "]" << ", Number of out-of-bound inserts [" << num_oob_insert 
            << "]")

  ASSERT_WITH_MSG(!(equal(num_init, uint32_t(0)) 
                  & (equal(num_kks_insert, uint32_t(0)) 
                  | equal(num_kks_insert, uint32_t(1)))), 
                  tostr<const char*>("Cannot generate kown-key-space inserts ")
                  + "when no data or only one data are initialized");

  // Prepare the out-of-bound keys
  std::vector<KT> kks_insert_keys;
  std::vector<KT> oob_insert_keys;
  kks_insert_keys.reserve(num_kks_insert);
  oob_insert_keys.reserve(num_oob_insert);
  for (uint32_t i = num_init + num_kks_insert; i < num_keys; ++ i) {
    oob_insert_keys.push_back(keys[i]);
  }

  // Shuffle the known-key-space data
  if (num_init + num_kks_insert > 2) {
    shuffle(keys, 1, num_init + num_kks_insert - 1);
    std::swap(keys[num_init - 1], keys[num_init + num_kks_insert - 1]);
    std::sort(keys.begin(), keys.begin() + num_init, 
      [](auto const& a, auto const& b) {
        return a < b;
    });
    for (uint32_t i = num_init; i < num_init + num_kks_insert; ++ i) {
      kks_insert_keys.push_back(keys[i]);
    }
  }

  // Prepare the data for bulk loading
  std::vector<KT> existing_keys;
  std::vector<Request<KT>> reqs;
  reqs.reserve(num_reqs + num_init);
  existing_keys.reserve(num_keys);
  for (int i = 0; i < num_init; ++ i) {
    reqs.push_back({kBulkLoad, keys[i]});
    existing_keys.push_back(keys[i]);
  }

  uint32_t num_act_query_keys = 0;
  uint32_t num_act_update_keys = 0;
  uint32_t num_act_remove_keys = 0;
  uint32_t num_act_insert_keys = 0;
  uint32_t num_act_kks_insert = 0;
  uint32_t num_act_oob_insert = 0;

  double cum_query_frac = query_frac;
  double cum_update_frac = query_frac + update_frac;
  double cum_remove_frac = query_frac + update_frac + remove_frac;

  // Generate the requests based on the read-fraction
  const uint64_t rand_limits = 1000000000;
  std::mt19937_64 op_gen(kSeed), domain_gen(kSeed), dist_gen(kSeed);

  ScrambledZipfianGenerator zipf_gen(num_init);
  std::uniform_int_distribution<> uniform_gen(0, num_init - 1);

  for (uint32_t i = 0, j = 0, k = 0; i < num_reqs; ++ i) {
    double op_frac = (op_gen() % rand_limits) * 1. / rand_limits;
    
    uint32_t idx = 0;
    if (op_frac < cum_remove_frac) {
      if (req_dist == "zipf") {
        idx = zipf_gen.nextValue();
      } else if (req_dist == "uniform") {
        idx = uniform_gen(dist_gen);
      } else {
        COUT_ERR("Unknown distribution type [" << req_dist << "]")
      }
      // idx = idx * existing_keys.size() * 1. / num_init;
      ASSERT_WITH_MSG(idx < existing_keys.size(), "Key index geneneration error");
    }

    if (op_frac < cum_query_frac) {
      reqs.push_back({kQuery, existing_keys[idx]});
      num_act_query_keys ++;
    } else if (op_frac < cum_update_frac) {
      reqs.push_back({kUpdate, existing_keys[idx]});
      num_act_update_keys ++;
    } else if (op_frac < cum_remove_frac) {
      reqs.push_back({kRemove, existing_keys[idx]});
      num_act_remove_keys ++;
    } else {
      double domain_frac = (domain_gen() % rand_limits) * 1. / rand_limits;
      num_act_insert_keys ++;
      if (domain_frac < kks_insert_frac) {
        if (j < kks_insert_keys.size()) {
          reqs.push_back({kInsert, kks_insert_keys[j]});
          ++ j;
          num_act_kks_insert ++;
        } else {
          break;
        }
      } else {
        if (k < oob_insert_keys.size()) {
          reqs.push_back({kInsert, oob_insert_keys[k]});
          ++ k;
          num_act_oob_insert ++;
        } else {
          break;
        }
      }
    }
  }

  check_workload(reqs);

  uint32_t num_act_reqs = num_act_query_keys + num_act_update_keys 
                          + num_act_remove_keys + num_act_insert_keys;

  COUT_INFO("Actual number of requests [" << num_act_reqs << "]\n" 
            << "\tActual number of queries [" << num_act_query_keys << "]\n" 
            << "\tActual number of updates [" << num_act_update_keys << "]\n" 
            << "\tActual number of removes [" << num_act_remove_keys << "]\n" 
            << "\tActual number of inserts [" << num_act_insert_keys << "]\n" 
            << "\t\tActual number of known-key-space inserts [" 
            << num_act_kks_insert << "]" 
            << ", Actual number of out-of-bound inserts [" 
            << num_act_oob_insert << "]")

  store_workload(workload_path, reqs);
}

}

#endif
