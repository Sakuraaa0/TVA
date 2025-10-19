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

# gflags
find_package(gflags REQUIRED)
# OpenMP
find_package(OpenMP REQUIRED)


#  PREFETCH default OFF（禁用）
option(PREFETCH "Enable prefetching" OFF)
if (PREFETCH)
    add_definitions(-DPREFETCH)
    message(STATUS "Prefetching is enabled.")
else ()
    message(STATUS "Prefetching is disabled.")
endif ()


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

if (PREFETCH)
    target_compile_definitions(CtGraph_demo PRIVATE PREFETCH)
    message(STATUS "Set PREFETCH for CtGraph_demo.")
endif ()

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

if (PREFETCH)
    target_compile_definitions(randomInsert_experiment PRIVATE PREFETCH)
    message(STATUS "Set PREFETCH for CtGraph_demo.")
endif ()


#-----------------------------Graphalytics--------------------------------------
add_executable(Graphalytics_experiment experiment/graphalytics.cc)
target_link_libraries(Graphalytics_experiment unordered_dense::unordered_dense src gflags OpenMP::OpenMP_CXX)
target_include_directories(Graphalytics_experiment PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

if (PREFETCH)
    target_compile_definitions(Graphalytics_experiment PRIVATE PREFETCH)
    message(STATUS "Set PREFETCH for CtGraph_demo.")
endif ()

#------------------------Mybench Experiment------------------------------------------
add_executable(Mybench_experiment experiment/Mybench.cc)
target_link_libraries(Mybench_experiment unordered_dense::unordered_dense src gflags)
target_include_directories(Mybench_experiment PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

#------------------------LDBC Experiment------------------------------------------
add_executable(LDBC_experiment experiment/LDBC.cc)
target_link_libraries(LDBC_experiment unordered_dense::unordered_dense src gflags)
target_include_directories(LDBC_experiment PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

add_executable(LDBC_test experiment/LDBC_test.cc)
target_link_libraries(LDBC_test unordered_dense::unordered_dense src gflags)
target_include_directories(LDBC_test PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)


add_executable(VersionChain_test src/VersionChain.cpp)
target_link_libraries(VersionChain_test unordered_dense::unordered_dense src gflags)
target_include_directories(VersionChain_test PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)

#----------------------------Hoptest------------------------------------------
add_executable(VersionChain_experiment main.cpp)


target_link_libraries(VersionChain_experiment unordered_dense::unordered_dense src gflags OpenMP::OpenMP_CXX)


target_include_directories(VersionChain_experiment PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils
)
#target_compile_options(CtGraph_demo PRIVATE -O3)

if (PREFETCH)
    target_compile_definitions(VersionChain_experiment PRIVATE PREFETCH)
    message(STATUS "Set PREFETCH for CtGraph_demo.")
endif ()

add_executable(TemporalChain_experiment src/ColumbTable.cpp)

target_link_libraries(TemporalChain_experiment src gflags)

target_include_directories(TemporalChain_experiment PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/utils)