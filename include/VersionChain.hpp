#ifndef CTGRAPH_VERSIONCHAIN_HPP
#define CTGRAPH_VERSIONCHAIN_HPP

#include <cstdint>
#include <forward_list>
#include <memory>
#include "types.hpp"

namespace ctgraph
{

    struct VersionChainEntry
    {
        uint64_t timestamp;
        int value;
        std::shared_ptr<FixString> property;

        VersionChainEntry(uint64_t ts, int val, std::shared_ptr<FixString> prop) :
            timestamp(ts), value(val), property(prop)
        {
        }
    };

    class VersionChain
    {
    private:
        std::forward_list<VersionChainEntry> chain_;

    public:
        VersionChain() = default;


        void insert(uint64_t timestamp, int value, std::shared_ptr<FixString> property)
        {
            chain_.push_front(VersionChainEntry(timestamp, value, property));
        }


        const VersionChainEntry* get_version(uint64_t timestamp) const
        {
            const VersionChainEntry* prev = nullptr;
            int count = 0;
            for (const auto& entry : chain_)
            {
                if (entry.timestamp <= timestamp)
                {
                    if (prev && prev->timestamp > timestamp)
                    {
                        return &entry;
                    }
                }
                prev = &entry;
                count++;
            }
            return nullptr;
        }


        const VersionChainEntry* get_latest() const { return chain_.empty() ? nullptr : &chain_.front(); }


        const std::forward_list<VersionChainEntry>& get_all_versions() const { return chain_; }
    };

} // namespace ctgraph

#endif // CTGRAPH_VERSIONCHAIN_HPP
