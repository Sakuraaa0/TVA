//
// Created by lwh on 24-12-25.
//

#ifndef CTGRAPH_DEMO_ID_TYPES_HPP
#define CTGRAPH_DEMO_ID_TYPES_HPP

#include <functional>
#include <type_traits>

#include "cast.hpp"


namespace hophash
{

#define STORAGE_DEFINE_ID_TYPE(name)                                                                                   \
    class name final                                                                                                   \
    {                                                                                                                  \
                                                                                                                       \
                                                                                                                       \
    public:                                                                                                            \
        /* Default constructor to allow serialization or preallocation. */                                             \
        name() = default;                                                                                              \
        explicit name(uint64_t id) : id_(id) {}                                                                        \
                                                                                                                       \
        static name FromUint(uint64_t id) { return name{id}; }                                                         \
        static name FromInt(int64_t id) { return name{utils::MemcpyCast<uint64_t>(id)}; }                              \
        uint64_t AsUint() const { return id_; }                                                                        \
        int64_t AsInt() const { return utils::MemcpyCast<int64_t>(id_); }                                              \
                                                                                                                       \
    private:                                                                                                           \
        uint64_t id_;                                                                                                  \
    };                                                                                                                 \
    static_assert(std::is_trivially_copyable<name>::value, "storage::" #name " must be trivially copyable!");          \
    inline bool operator==(const name& first, const name& second) { return first.AsUint() == second.AsUint(); }        \
    inline bool operator!=(const name& first, const name& second) { return first.AsUint() != second.AsUint(); }        \
    inline bool operator<(const name& first, const name& second) { return first.AsUint() < second.AsUint(); }          \
    inline bool operator>(const name& first, const name& second) { return first.AsUint() > second.AsUint(); }          \
    inline bool operator<=(const name& first, const name& second) { return first.AsUint() <= second.AsUint(); }        \
    inline bool operator>=(const name& first, const name& second) { return first.AsUint() >= second.AsUint(); }

    STORAGE_DEFINE_ID_TYPE(Gid);


#undef STORAGE_DEFINE_ID_TYPE
} // namespace hophash

namespace std
{
    template <>
    struct hash<hophash::Gid>
    {
        std::size_t operator()(const hophash::Gid& gid) const noexcept { return std::hash<uint64_t>()(gid.AsUint()); }
    };
} // namespace std

#endif // CTGRAPH_DEMO_ID_TYPES_HPP
