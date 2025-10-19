set(SRC
        CtGraph.cc
        CtStore.cc
        MemTable.cc
        flags.cc
        ColumbTable.cpp
        VersionChain.cpp)
add_library(src STATIC
     ${SRC}
)

set_target_properties(src PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(src unordered_dense::unordered_dense)