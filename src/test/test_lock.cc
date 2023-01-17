#include "common.h"

namespace po = boost::program_options;
using namespace aflipara;

void check_options(const po::variables_map& vm, 
                   const std::vector<std::string>& options) {
  for (auto op : options) {
    if (!vm.count(op)) {
      COUT_ERR("--" << op << " option required")
    }
  }
}

uint32_t dummy_operation() {
  uint32_t sum = 0;
  for (uint32_t i = 0; i < 1000000; ++ i) {
    sum += i;
  }
  return sum;
}

double test_asm_spin_lock(uint32_t num_iterations) {
  auto start = TIME_LOG;
  volatile uint8_t status = 0;
  auto lock = [&status]() {
    uint8_t unlocked = 0, locked = 1;
    while (unlikely(cmpxchgb((uint8_t*)&status, unlocked, locked) != unlocked))
      ;
  };
  auto unlock = [&status]() {
    status = 0;
  };
  for (uint32_t i = 0; i < num_iterations; ++ i) {
    lock();
    dummy_operation();
    unlock();
  }
  double duration = TIME_IN_NANO_SECOND(start, TIME_LOG);
  return duration / num_iterations;
}

double test_pthread_lock(uint32_t num_iterations) {
  auto start = TIME_LOG;
  pthread_mutex_t mutex;
  pthread_mutex_init(&mutex, 0);
  for (uint32_t i = 0; i < num_iterations; ++ i) {
    pthread_mutex_lock(&mutex);
    dummy_operation();
    pthread_mutex_unlock(&mutex);
  }
  double duration = TIME_IN_NANO_SECOND(start, TIME_LOG);
  return duration / num_iterations;
}

double test_stl_lock(uint32_t num_iterations) {
  auto start = TIME_LOG;
  std::mutex mutex;
  for (uint32_t i = 0; i < num_iterations; ++ i) {
    mutex.lock();
    dummy_operation();
    mutex.unlock();
  }
  double duration = TIME_IN_NANO_SECOND(start, TIME_LOG);
  return duration / num_iterations;
}

class STLSpinLock
{
public:
    STLSpinLock() : status(false) {}

    void lock() {
      while (true) {
        if (!status.exchange(true, std::memory_order_acquire)) {
          break;
        }
        while (status.load(std::memory_order_relaxed)) {
          __builtin_ia32_pause();
        }
      }
    }

    inline void unlock() { status.store(false); }

private:
    std::atomic<bool> status;
};

double test_stl_spin_lock(uint32_t num_iterations) {
  auto start = TIME_LOG;
  STLSpinLock spin_lock;
  for (uint32_t i = 0; i < num_iterations; ++ i) {
    spin_lock.lock();
    dummy_operation();
    spin_lock.unlock();
  }
  double duration = TIME_IN_NANO_SECOND(start, TIME_LOG);
  return duration / num_iterations;
}


int main(int argc, char* argv[]) {
  po::options_description desc("Allowed options");
  desc.add_options()
    ("num_iters", po::value<uint32_t>(), 
     "# of iterations")
  ;

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
  } catch (...) {
    COUT_ERR("Unrecognized parameters, please use --num_iters");
  }
  po::notify(vm);

  uint32_t num_iterations = 100000;
  if (vm.count("num_iters")) {
    num_iterations = vm["num_iters"].as<uint32_t>();
  }
  COUT_INFO("# Iterations: " << num_iterations)
  COUT_INFO("\tAvg latency of asm spin lock:\t" 
            << test_asm_spin_lock(num_iterations) << " ns")
  COUT_INFO("\tAvg latency of pthread lock:\t" 
            << test_pthread_lock(num_iterations) << " ns")
  COUT_INFO("\tAvg latency of stl lock:\t" 
            << test_stl_lock(num_iterations) << " ns")
  COUT_INFO("\tAvg latency of stl spin lock:\t" 
            << test_stl_spin_lock(num_iterations) << " ns")
  return 0;
}

