set(SRC
        CtGraph.cc
        CtStore.cc
        MemTable.cc
        flags.cc
        ColumbTable.cpp
        VersionChain.cpp
        persistence/RocksDBStore.cc)
add_library(src STATIC
     ${SRC}
)

set_target_properties(src PROPERTIES LINKER_LANGUAGE CXX)

find_package(RocksDB QUIET)
if(RocksDB_FOUND)
    target_link_libraries(src unordered_dense::unordered_dense RocksDB::rocksdb)
    target_compile_definitions(src PUBLIC HAS_ROCKSDB)
elseif(EXISTS "/usr/lib/librocksdb.so")
    target_link_libraries(src unordered_dense::unordered_dense rocksdb)
    target_compile_definitions(src PUBLIC HAS_ROCKSDB)
else()
    target_link_libraries(src unordered_dense::unordered_dense)
endif()