cmake_minimum_required(VERSION 3.12)
project(KV-Evaluator VERSION 1.0 LANGUAGES CXX)

# specify c/c++ standard
if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)
endif(NOT CMAKE_CXX_STANDARD)

list(APPEND CMAKE_PREFIX_PATH "${PROJECT_SOURCE_DIR}/lib/libmkl")

set(SRC_DIR "${PROJECT_SOURCE_DIR}/src")

# Configuration for ALEX
if (MSVC)
  set(CMAKE_CXX_FLAGS "/O2 /arch:AVX2 /W1 /EHsc")
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Intel")
  set(CMAKE_CXX_FLAGS "-O3 -xHost -ggdb3 -pthread")
else ()
  set(CMAKE_CXX_FLAGS "-O3 -march=native -ggdb3 -pthread")
endif ()

include_directories(
  ${SRC_DIR}
)

add_executable(sample "${SRC_DIR}/util/sample_keys.cc")
add_executable(test_afli_para "${SRC_DIR}/test/test_afli_para.cc")
add_executable(test_nfl_para "${SRC_DIR}/test/test_nfl_para.cc")
add_executable(test_lock "${SRC_DIR}/test/test_lock.cc")
add_executable(test_linear_model "${SRC_DIR}/test/test_linear_model.cc")
add_executable(compare_data "${SRC_DIR}/test/compare_data.cc")

find_package(MKL)
if (MKL_FOUND)
  include_directories(${MKL_INCLUDE_DIR})
  target_link_libraries(test_afli_para ${MKL_LIBRARIES})
  target_link_libraries(test_nfl_para ${MKL_LIBRARIES})
else ()
  message(WARNING "MKL libs not found")
endif ()

find_package(OpenMP REQUIRED)
if (OPENMP_FOUND)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
else ()
  message(WARNING "OpenMP libs not found")
endif ()

find_package(Boost COMPONENTS program_options REQUIRED)
if (Boost_FOUND)
  include_directories(${Boost_INCLUDE_DIRS})
  target_link_libraries(sample ${Boost_LIBRARIES})
  target_link_libraries(test_afli_para ${Boost_LIBRARIES})
  target_link_libraries(test_nfl_para ${Boost_LIBRARIES})
  target_link_libraries(test_lock ${Boost_LIBRARIES})
  target_link_libraries(test_linear_model ${Boost_LIBRARIES})
  target_link_libraries(compare_data ${Boost_LIBRARIES})
else ()
  message(WARNING "Boost libs are not found")
endif ()