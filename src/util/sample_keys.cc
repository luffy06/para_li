#include "util/workload.h"
#include "core/common.h"

namespace po = boost::program_options;
using namespace aflipara;

template<typename KT>
void sample_keys(const std::vector<KT>& keys, double sample_ratio, 
                 std::string output_path) {
  uint32_t num_samples = keys.size() * sample_ratio;
  std::vector<uint32_t> ids;
  for (uint32_t i = 0; i < keys.size(); ++ i) {
    ids.push_back(i);
  }
  shuffle(ids, 0, num_samples);
  std::ofstream out(output_path, std::ios::out);
  ASSERT_WITH_MSG(out.is_open(), "File [" << output_path << "] does not exist");
  for (int i = 0; i < num_samples; ++ i) {
    out << std::fixed << std::setprecision(std::numeric_limits<KT>::digits10) 
        << keys[ids[i]] << std::endl;
  }
  out.close();
}

int main(int argc, char* argv[]) {
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", (tostr("example: ./nf_convert ") 
     + "--key_path books-200M-zipf.bin " 
     + "--key_type uint64 --sample_ratio 0.1 --output_dir nf_train").data())
    ("key_path", po::value<std::string>(), 
     "the path of key set")
    ("key_type", po::value<std::string>(), 
     "the key type of workload, e.g., double, int32, int64")
    ("sample_ratio", po::value<double>(), 
     "the ratio of sampling keys from the key set")
    ("output_dir", po::value<std::string>(), 
     "the output directory")
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

  check_options(vm, {"key_path", "key_type", "output_dir"});
  std::string key_path = vm["key_path"].as<std::string>();
  std::string key_type = vm["key_type"].as<std::string>();
  std::string output_dir = vm["output_dir"].as<std::string>();
  double sample_ratio = 1;
  if (vm.count("sample_ratio")) {
    sample_ratio = vm["sample_ratio"].as<double>();
  }
  std::string output_path = output_dir + "/" 
                            + (*(split(key_path, '/').rbegin()));
  std::string suffix = std::string("-") + tostr(uint32_t(sample_ratio * 100)) 
                       + "P-training.txt";
  output_path.replace(output_path.find(".bin"), 4, suffix);
  if (key_type == "double") {
    std::vector<double> keys;
    load_keyset(key_path, keys);
    sample_keys(keys, sample_ratio, output_path);
  } else if (key_type == "int64") {
    std::vector<int64_t> keys;
    load_keyset(key_path, keys);
    sample_keys(keys, sample_ratio, output_path);
  } else if (key_type == "uint64") {
    std::vector<uint64_t> keys;
    load_keyset(key_path, keys);
    sample_keys(keys, sample_ratio, output_path);
  } else {
    COUT_ERR("Unsupported key type\t" << key_type)
  }
  return 0;
}