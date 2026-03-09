cmake_minimum_required(VERSION 3.16)
project(CtGraph)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3")


set(CMAKE_CXX_STANDARD 20)
#set(CMAKE_CXX_COMPILER "g++")

message(STATUS "CMAKE_SOURCE_DIR: ${CMAKE_SOURCE_DIR}")

#查找unordered_dense
set(CMAKE_PREFIX_PATH ${CMAKE_SOURCE_DIR}/thirdparty/unordered_dense/build/build/unordered_dense_install/lib/cmake/unordered_dense/)
find_package(unordered_dense CONFIG REQUIRED)

#查找gflags
find_package(gflags REQUIRED)
#查找OpenMP
find_package(OpenMP REQUIRED)


# 设置 PREFETCH 选项，默认为 OFF（禁用）
option(PREFETCH "Enable prefetching" OFF)

# 如果开启了 PREFETCH，则定义宏
if(PREFETCH)
    add_definitions(-DPREFETCH)
    message(STATUS "Prefetching is enabled.")
else()
    message(STATUS "Prefetching is disabled.")
endif()


include_directories(
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils
        ${CMAKE_SOURCE_DIR}/experiment
)
add_executable(CtGraph_demo main.cpp)



add_subdirectory(src)
add_subdirectory(utils)



target_link_libraries(CtGraph_demo unordered_dense::unordered_dense src gflags OpenMP::OpenMP_CXX)




target_include_directories(CtGraph_demo PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils
)
#target_compile_options(CtGraph_demo PRIVATE -O3)

if(PREFETCH)
    target_compile_definitions(CtGraph_demo PRIVATE PREFETCH)
    message(STATUS "Set PREFETCH for CtGraph_demo.")
endif()

add_executable(Column_demo src/ColumbTable.cpp)

target_link_libraries(Column_demo src gflags)

target_include_directories(Column_demo PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

#------------------------Shuffle data------------------------
add_executable(shuffle_data experiment/shuffledata.cc)


#------------------------randomInsert Experiment------------------------------------------
add_executable(degree_count experiment/count_degree.cc)

add_executable(randomInsert_experiment experiment/randomInsert.cpp)
target_link_libraries(randomInsert_experiment unordered_dense::unordered_dense src gflags OpenMP::OpenMP_CXX)
target_include_directories(randomInsert_experiment PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

if(PREFETCH)
    target_compile_definitions(randomInsert_experiment PRIVATE PREFETCH)
    message(STATUS "Set PREFETCH for CtGraph_demo.")
endif()


#-----------------------------Graphalytics--------------------------------------
add_executable(Graphalytics_experiment experiment/graphalytics.cc)
target_link_libraries(Graphalytics_experiment unordered_dense::unordered_dense src gflags OpenMP::OpenMP_CXX)
target_include_directories(Graphalytics_experiment PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

if(PREFETCH)
    target_compile_definitions(Graphalytics_experiment PRIVATE PREFETCH)
    message(STATUS "Set PREFETCH for CtGraph_demo.")
endif()

#------------------------Mybench Experiment------------------------------------------
add_executable(Mybench_experiment experiment/Mybench.cc)
target_link_libraries(Mybench_experiment  unordered_dense::unordered_dense src gflags)
target_include_directories(Mybench_experiment PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

#------------------------Mybench Persist Experiment------------------------------------------
find_package(RocksDB QUIET)
add_executable(Mybench_persist experiment/Mybench_persist.cc src/persistence/RocksDBStore.cc)
if(RocksDB_FOUND)
    target_link_libraries(Mybench_persist unordered_dense::unordered_dense src gflags RocksDB::rocksdb)
    target_compile_definitions(Mybench_persist PRIVATE HAS_ROCKSDB)
elseif(EXISTS "/usr/lib/librocksdb.so")
    target_link_libraries(Mybench_persist unordered_dense::unordered_dense src gflags rocksdb)
    target_compile_definitions(Mybench_persist PRIVATE HAS_ROCKSDB)
else()
    target_link_libraries(Mybench_persist unordered_dense::unordered_dense src gflags)
    message(WARNING "RocksDB not found, FULL_MODE will not work")
endif()
target_include_directories(Mybench_persist PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

#------------------------Mybench No Temporal Experiment------------------------------------------
add_executable(Mybench_no_temporal experiment/Mybench_no_temporal.cc)
target_link_libraries(Mybench_no_temporal  unordered_dense::unordered_dense src gflags)
target_include_directories(Mybench_no_temporal PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

#------------------------LDBC Experiment------------------------------------------
add_executable(LDBC_experiment experiment/LDBC.cc)
target_link_libraries(LDBC_experiment  unordered_dense::unordered_dense src gflags)
target_include_directories(LDBC_experiment PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

add_executable(LDBC_test experiment/LDBC_test.cc)
target_link_libraries(LDBC_test  unordered_dense::unordered_dense src gflags)
target_include_directories(LDBC_test PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

#------------------------Threshold Test------------------------------------------
add_executable(threshold_test experiment/threshold_test.cc)
target_link_libraries(threshold_test  unordered_dense::unordered_dense src gflags)
target_include_directories(threshold_test PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)


add_executable(VersionChain_test src/VersionChain.cpp)
target_link_libraries(VersionChain_test  unordered_dense::unordered_dense src gflags)
target_include_directories(VersionChain_test PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

#----------------------------Hoptest------------------------------------------
add_executable(Hoptest_demo hoptest.cpp)





target_link_libraries(Hoptest_demo unordered_dense::unordered_dense src gflags OpenMP::OpenMP_CXX)




target_include_directories(Hoptest_demo PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils
)
#target_compile_options(CtGraph_demo PRIVATE -O3)

if(PREFETCH)
    target_compile_definitions(Hoptest_demo PRIVATE PREFETCH)
    message(STATUS "Set PREFETCH for Hoptest_demo.")
endif()


#----------------------------Zipf4Update------------------------------------------
add_executable(Zipf4Update zipf4update.cpp)





target_link_libraries(Zipf4Update unordered_dense::unordered_dense src gflags OpenMP::OpenMP_CXX)




target_include_directories(Zipf4Update PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils
)
#target_compile_options(CtGraph_demo PRIVATE -O3)

if(PREFETCH)
    target_compile_definitions(Zipf4Update PRIVATE PREFETCH)
    message(STATUS "Set PREFETCH for Zipf4Update.")
endif()

#------------------------Mybenchthread Experiment------------------------------------------
add_executable(Mybenchthread_experiment experiment/Mgbenchthread.cc)
target_link_libraries(Mybenchthread_experiment  unordered_dense::unordered_dense src gflags)

target_include_directories(Mybenchthread_experiment PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)


#------------------------gMark Experiment------------------------------------------
add_executable(Gmark_experiment experiment/GmarkBench.cc)
target_link_libraries(Gmark_experiment  unordered_dense::unordered_dense src gflags)
target_include_directories(Gmark_experiment PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

