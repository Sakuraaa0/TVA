//
// Created by lwh on 25-1-2.
//

#ifndef HOPSCOTCHHASH_HPP
#define HOPSCOTCHHASH_HPP

#include <ankerl/unordered_dense.h>
#include <chrono>
#include <edge.hpp>
#include <emmintrin.h>
#include <functional> // for std::hash
#include <immintrin.h> //  AVX-512
#include <iostream>
#include <memory>
#include <ostream>
#include <stdexcept> // for std::runtime_error
#include <types.hpp>
#include <vector>
#include "fixstring.hpp"

#include "flags.hpp"
#include "id_types.hpp"
#include "prefetch.hpp"
#include "types.hpp"

namespace hophash
{

    using HopInfoSize = int32_t;

    struct HashNode
    {
        ctgraph::Edge edge_;
        ctgraph::SequenceNumber_t end_ts_;
        bool occupied = false;

        HashNode()
        {
            edge_ = {};
            end_ts_ = {};
            occupied = false;
        }

        HashNode(ctgraph::VertexId_t to_gid, ctgraph::EdgeProperty_t* = nullptr,
                 ctgraph::SequenceNumber_t start_ts_ = 0,
                 ctgraph::SequenceNumber_t end_ts_ = ctgraph::INVALID_TIMESTAMP, bool occupied = true) :
            end_ts_(end_ts_), occupied(occupied)
        {
            edge_ = ctgraph::Edge(to_gid, start_ts_);
        }
        HashNode(ctgraph::Edge edge, ctgraph::SequenceNumber_t end_ts_ = 0, bool occupied = true) :
            edge_(edge), end_ts_(end_ts_), occupied(occupied)
        {
        }

        bool operator==(const HashNode& other) const { return edge_ == other.edge_; }

        // operator<
        bool operator<(const HashNode& other) const { return edge_ < other.edge_; }
#ifdef PREFETCH
    } __attribute__((aligned(64))); // Enforcing a 64-byte alignment facilitates prefetching
#else
    };
#endif

    struct HashNodeHash
    {
        std::size_t operator()(const HashNode& node) const
        {
            return std::hash<ctgraph::VertexId_t>()(node.edge_.destination());
        }

        friend bool operator<(const HashNode& first, const HashNode& second) { return first.edge_ < second.edge_; }
    };

    class HopscotchHashTable
    {
    public:
        explicit HopscotchHashTable(size_t capacity = 8192 << 1, size_t maxHopDistance = 8) :
            capacity_(capacity), maxHopDistance_(maxHopDistance)
        {
            size_t table_size = capacity_ << 1;
            table_.resize(table_size);
            hopInfo_.resize(table_size);
            backInfo_.resize(table_size);
            control_.resize(table_size);

            // SIMD
#pragma omp parallel for
            //  hopInfo_  backInfo_ (int32_t，4)
            for (size_t i = 0; i < table_size; i += 4)
            {
                //  SSE  hopInfo_ (int32_t)
                __m128i hop_init = _mm_set1_epi32(-1);
                _mm_stream_si128((__m128i*)&hopInfo_[i], hop_init);

                //  SSE  backInfo_ (int32_t)
                __m128i back_init = _mm_set1_epi32(ctgraph::BACKEMPTY);
                _mm_stream_si128((__m128i*)&backInfo_[i], back_init);
            }

            //  control_ (uint8_t，16)
#pragma omp parallel for
            for (size_t i = 0; i < table_size; i += 16)
            {
                //  SSE  control_ (uint8_t)
                __m128i control_init = _mm_set1_epi8(ctgraph::HOPEMPTY);
                _mm_stream_si128((__m128i*)&control_[i], control_init);
            }

            size_t remaining = table_size % 4;
            if (remaining > 0)
            {
                size_t base = (table_size / 4) * 4;
                for (size_t i = 0; i < remaining; ++i)
                {
                    hopInfo_[base + i] = -1;
                    backInfo_[base + i] = ctgraph::BACKEMPTY;
                }
            }

            remaining = table_size % 16;
            if (remaining > 0)
            {
                size_t base = (table_size / 16) * 16;
                for (size_t i = 0; i < remaining; ++i)
                {
                    control_[base + i] = ctgraph::HOPEMPTY;
                }
            }
            latestValuePtr_.reserve(capacity_);
        }

        void ChangeCapacity(size_t capacity)
        {
            size_t old_capacity = capacity_;
            capacity_ = capacity;
            size_t new_size = capacity_ << 1;
            table_.resize(new_size);
            hopInfo_.resize(new_size);
            backInfo_.resize(new_size);
            control_.resize(new_size);
            std::fill(control_.begin() + old_capacity * 2, control_.end(), ctgraph::HOPEMPTY);
            std::fill(hopInfo_.begin() + old_capacity * 2, hopInfo_.end(), -1);
            std::fill(backInfo_.begin() + old_capacity * 2, backInfo_.end(), ctgraph::BACKEMPTY);
            std::fill(table_.begin() + old_capacity * 2, table_.end(), HashNode());
        }

        static uint64_t getCurrentTimeInMicroseconds()
        {
            auto now = std::chrono::system_clock::now();
            auto duration = now.time_since_epoch();
            auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
            return static_cast<uint64_t>(microseconds);
        }

        void setHopInfoInitial(size_t idx, HopInfoSize hop, bool is_initial)
        {
            hopInfo_[idx] = (hop << 1) | static_cast<HopInfoSize>(is_initial);
        }
        void setHopInfo(size_t idx, HopInfoSize hop) { hopInfo_[idx] = (hop << 1) | (hopInfo_[idx] & 1); }
        void setInitial(size_t idx, bool is_initial)
        {
            hopInfo_[idx] = (hopInfo_[idx] & ~1) | static_cast<HopInfoSize>(is_initial);
        }

        HopInfoSize getHopInfo(size_t idx) { return hopInfo_[idx] >> 1; }

        bool getInitial(size_t idx) { return hopInfo_[idx] & 0x1; }

        void insertWithTs(ctgraph::Edge& to_gid)
        {
            auto it = latestValuePtr_.find(to_gid.destination());
            bool exist = (it != latestValuePtr_.end());
            auto hash_value = mixed_hash(to_gid.destination());
            size_t idx = exist ? it->second : hash_value & (capacity_ - 1);

            auto freeIdxOpt = findFreeBucket(idx);
            while (!freeIdxOpt.has_value())
            {
                ChangeCapacity(capacity_ << 1);
                freeIdxOpt = findFreeBucket(hash_value & (capacity_ - 1));
            }
            size_t freeIdx = freeIdxOpt.value();

            if (exist)
            {
                table_[idx].end_ts_ = to_gid.sequence();
            }
            else
            { // 如果不存在则插入任意位置均可
                table_[freeIdx] = HashNode(to_gid);
                // hopInfo_[idx] = 0x1;
                setHopInfoInitial(freeIdx, 0, true);
                latestValuePtr_[to_gid.destination()] = freeIdx;
                control_[freeIdx] = H2(hash_value);
                backInfo_[freeIdx] = ctgraph::BACKEMPTY;
                return;
            }
            // if no confilict，insert **seem impossible**
            // if (freeIdx == idx) {
            //     table_[idx] = HashNode(to_gid);
            //     // hopInfo_[idx] = 0;
            //     setHopInfoInitial(idx,0,false);
            //     latestValuePtr_[to_gid.destination()] = idx;
            //     control_[idx] = H2(hash_value);
            //     return;
            // }

            // If the idle buckets are not within the hop range, adjustments should be made
            while (freeIdx > idx + maxHopDistance_)
            {
                freeIdxOpt = shift(idx + 1, freeIdx);
                if (!freeIdxOpt.has_value())
                {
                    // throw std::runtime_error("Unable to resolve conflict within max hop
                    // distance.");
                    maxHopDistance_++;
                }
                else
                {
                    freeIdx = freeIdxOpt.value();
                    break;
                }
            }

            // Inserting elements and updating hop information
            table_[freeIdx] = HashNode(to_gid);
            // hopInfo_[freeIdx] = freeIdx - idx;
            setHopInfoInitial(freeIdx, freeIdx - idx, false);
            latestValuePtr_[to_gid.destination()] = freeIdx;
            control_[freeIdx] = H2(hash_value);
            backInfo_[idx] = freeIdx - idx;
        }

        void insert(ctgraph::VertexId_t to_gid)
        {
            auto time = getCurrentTimeInMicroseconds();

            auto it = latestValuePtr_.find(to_gid);
            bool exist = (it != latestValuePtr_.end());
            auto hash_value = mixed_hash(to_gid);
            size_t idx = exist ? it->second : hash_value & (capacity_ - 1);

            auto freeIdxOpt = findFreeBucket(idx);
            if (!freeIdxOpt.has_value())
            {
                throw std::runtime_error("Hash table is full!");
            }
            size_t freeIdx = freeIdxOpt.value();

            if (exist)
            {
                table_[idx].end_ts_ = time;
            }
            else
            {
                table_[freeIdx] = HashNode(to_gid);
                // hopInfo_[idx] = 0x1;
                setHopInfoInitial(freeIdx, 0, true);
                latestValuePtr_[to_gid] = freeIdx;
                control_[freeIdx] = H2(hash_value);
                backInfo_[freeIdx] = ctgraph::BACKEMPTY;
                return;
            }
            // If there are no conflicts, insert directly
            if (freeIdx == idx)
            {
                table_[idx] = {to_gid, nullptr, time, 0, true};
                // hopInfo_[idx] = 0;
                setHopInfoInitial(idx, 0, true);
                latestValuePtr_[to_gid] = idx;
                control_[freeIdx] = H2(hash_value);
                return;
            }

            // If the idle buckets are not within the hop range, adjustments should be made
            while (freeIdx > idx + maxHopDistance_)
            {
                freeIdxOpt = shift(idx + 1, freeIdx);
                if (!freeIdxOpt.has_value())
                {
                    maxHopDistance_++;
                }
                else
                {
                    freeIdx = freeIdxOpt.value();
                    break;
                }
            }

            // Insert elements and update hop information
            table_[freeIdx] = {to_gid, nullptr, time, 0, true};
            setHopInfoInitial(freeIdx, freeIdx - idx, false);
            latestValuePtr_[to_gid] = freeIdx;
            control_[freeIdx] = H2(hash_value);
            backInfo_[idx] = freeIdx - idx;
        }

        ctgraph::Status get_latest(ctgraph::VertexId_t to_gid, ctgraph::EdgeProperty_t*& property)
        {
            if (latestValuePtr_.find(to_gid) == latestValuePtr_.end())
            {
                return ctgraph::Status::kNotFound;
            }
            if (table_[latestValuePtr_[to_gid]].edge_.marker())
            {
                return ctgraph::Status::kDelete;
            }
            property = &table_[latestValuePtr_[to_gid]].edge_.property();
            return ctgraph::Status::kOk;
        }

        ctgraph::Status getVersionInRange(ctgraph::VertexId_t to_gid, ctgraph::SequenceNumber_t target_ts,
                                          ctgraph::EdgeProperty_t*& property)
        {
            auto hash_value = mixed_hash(to_gid);
            size_t start_idx = hash_value & (capacity_ - 1);
            size_t end_idx = latestValuePtr_.find(to_gid)->second;

            // If no elements are found
            if (start_idx > end_idx)
            {
                start_idx = 0;
            }

            while (start_idx <= end_idx)
            {
                size_t mid_idx = (start_idx + end_idx) / 2;
                if (mid_idx + 16 >= capacity_ << 1)
                {
                    mid_idx = (capacity_ << 1) - 16;
                }
                uint64_t match_result = 0;
                // Use match to check the nearby 16 positions
                if (FLAGS_simd)
                {
                    match_result = match(mid_idx, H2(hash_value)); // H2(to_gid)
                }
                else
                {
                    match_result = 0xFFFF;
                }

                while (match_result)
                {
                    int pos = first_pos(match_result);
                    size_t idx = (mid_idx + pos) % (capacity_ << 1);
                    next_found(&match_result);

                    const HashNode& node = table_[idx];

                    // If the node meets the target timestamp range
                    if (node.occupied && node.edge_.destination() == to_gid)
                    {
                        if (node.edge_.sequence() <= target_ts && (target_ts <= node.end_ts_ || node.end_ts_ == 0))
                        {
                            property = &node.edge_.property();
                            if (node.edge_.marker())
                            {
                                return ctgraph::kDelete;
                            }
                            else
                            {
                                return ctgraph::kOk;
                            }
                        }

                        // If the start timestamp of the current node is already greater than the target timestamp,
                        // subsequent nodes cannot possibly satisfy the conditions
                        if (node.edge_.sequence() > target_ts)
                        {
                            end_idx = mid_idx - 1;
                            break;
                        }
                    }
                }
                if (mid_idx + 16 > end_idx && mid_idx <= end_idx)
                {
                    break;
                }
                if (mid_idx < end_idx)
                {
                    if (mid_idx >= start_idx)
                    {
                        start_idx = mid_idx + 1;
                    }
                    else
                    {
                        start_idx += 1;
                    }
                }
            }
            return ctgraph::kNotFound;
        }

        bool get_edges(std::vector<ctgraph::Edge>& edges)
        {
            edges.clear();
            edges.reserve(latestValuePtr_.size());
            int cnt = latestValuePtr_.size();
            // Iterate using an iterator and incorporate boundary checks
            auto it = latestValuePtr_.begin();
            for (int i = 0; i < cnt; i++)
            {
                edges.emplace_back(table_[it->second].edge_);
                it++;
            }
            return true;
        }

        bool get_edges_withTs(std::vector<ctgraph::Edge>& edges, ctgraph::SequenceNumber_t ts)
        {
            edges.clear();
            edges.reserve(latestValuePtr_.size());
            int cnt = latestValuePtr_.size();
            // Iterate using an iterator and incorporate boundary checks
            auto it = latestValuePtr_.begin();
            for (int i = 0; i < cnt; i++)
            {
                ctgraph::EdgeProperty_t* property = nullptr;
                getVersionInRange(it->first, ts, property);
                if (property != nullptr)
                {
                    ctgraph::Edge edge = ctgraph::Edge(it->first, 0, false, property);
                    edges.emplace_back(edge);
                }
                it++;
            }
            return true;
        }

        /**
         * Compares for equality using SIMD: Four CPU instructions can compare 16 pieces of data at once.
         * Usage:
         *   for (uint64_t found = match(control, h2); found; next_pos(&found)) {
         *       int pos = first_pos(found);
         *       assert(control[pos] == h2);
         *   }
         *   !!! When using this, ensure externally that there are 16 remaining positions.
         * @param control   The stored control_byte
         * @param c         The character to compare
         * @return          Returns a 16-bit integer. If a character in control matches c,
         *                  the corresponding bit will be 1; otherwise, it will be 0. For example,
         *                  if the 1st and 5th characters in control match c, the 1st and 5th bits
         *                  in the return value will be 1.
         */
        uint64_t match(size_t i, char c)
        {
            // Convert control to __m128i. Note: 16-byte alignment is required.
            // Ensure that there are at least 16 bytes from index i to i + 16
            assert(i + 16 <= control_.size());

            // Use alignas(16) to guarantee memory alignment
            alignas(16) uint8_t aligned_buffer[16];
            // Copy 16 bytes from control_ starting at position i into aligned_buffer
            std::memcpy(aligned_buffer, &control_[i], 16);

            // Use __m128i to process 16 bytes of data
            __m128i v = *(__m128i*)aligned_buffer; // Use the aligned buffer
            // Set every byte in __m128i w to c (broadcast c 16 times)
            __m128i w = _mm_set1_epi8(c);
            // Compare v and w byte by byte; matched bytes become 0xff, unmatched become 0x00
            __m128i x = _mm_cmpeq_epi8(v, w);
            // Convert the sign bits (most significant bits) of each byte to an int
            return _mm_movemask_epi8(x);
        }

        // Return the position of the least significant bit
        // that is set to 1 in the binary representation of an integer x of type uint64_t
        int first_pos(uint64_t x) { return __builtin_ctzll(x); }
        // Clear the least significant 1 in an unsigned 64-bit integer x
        void next_found(uint64_t* x) { *x &= (*x - 1); }

        uint8_t H2(size_t hash_value) const
        {
            return static_cast<uint8_t>(hash_value >> 57); // Extract the highest seven bit
        }

    private:
        size_t capacity_;
        size_t maxHopDistance_;
        std::vector<HashNode> table_;
        std::vector<HopInfoSize> hopInfo_; // The least significant bit indicates whether it is initial; 0 represents
                                           // no, and 1 represents yes
        std::vector<HopInfoSize> backInfo_;
        ankerl::unordered_dense::v4_5_0::hash<ctgraph::VertexId_t> m_hash_;
        std::unordered_map<ctgraph::VertexId_t, size_t> latestValuePtr_;
        // 0-127: Indicates the 7-bit hash value when an element is present
        // 0x80: Empty
        // 0xFE: Deleted
        std::vector<ctgraph::ControlType> control_;

        std::chrono::high_resolution_clock::time_point start_time_;

    public:
        double execution_time_ = 0.0;

        [[nodiscard]] std::unordered_map<ctgraph::VertexId_t, size_t> latest_value_ptr() const
        {
            return latestValuePtr_;
        }

        size_t get_max_hop_distance() const { return maxHopDistance_; }

    private:
        template <template <class...> class Op, class... Args>
        static constexpr bool is_detected_v = ankerl::unordered_dense::detail::is_detected<Op, Args...>::value;

        using GidType = hophash::Gid;
        using HashType = ankerl::unordered_dense::v4_5_0::hash<GidType, void>;

        // The goal of mixed_hash is to always produce a high quality 64bit hash.
        template <typename K>
        [[nodiscard]] constexpr auto mixed_hash(K const& key) const -> uint64_t
        {
            if constexpr (is_detected_v<ankerl::unordered_dense::detail::detect_avalanching, HashType>)
            {
                // we know that the hash is good because is_avalanching.
                if constexpr (sizeof(decltype(m_hash(key))) < sizeof(uint64_t))
                {
                    // 32bit hash and is_avalanching => multiply with a constant to
                    // avalanche bits upwards
                    return m_hash(key) * UINT64_C(0x9ddfea08eb382d69);
                }
                else
                {
                    // 64bit and is_avalanching => only use the hash itself.
                    return m_hash(key);
                }
            }
            else
            {
                // not is_avalanching => apply wyhash
                return ankerl::unordered_dense::detail::wyhash::hash(m_hash_(key));
            }
        }

        size_t hash(ctgraph::VertexId_t gid) const
        {
            auto hashvalue = mixed_hash(gid);
            // Query the remainder of capacity_ as the hash value
            return hashvalue & (capacity_ - 1);
        }

        std::optional<size_t> findFreeBucket(size_t startIdx)
        {
            while (startIdx < capacity_ << 1)
            {
                size_t idx = (startIdx) % (capacity_ << 1);
                if (!table_[idx].occupied)
                {
                    return idx;
                }
                startIdx++;
            }
            return std::nullopt;
        }

        /**
         * Moves a batch of data forward by one position.
         * @param end_idx    The index of the last element in the batch.
         * @param free_idx   The index of the available (free) slot to jump to.
         * @return           The new index of the free slot after the move.
         */
        void startTimer() { start_time_ = std::chrono::high_resolution_clock::now(); }

        void stopTimer()
        {
            auto end_time = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> duration = end_time - start_time_;
            execution_time_ += duration.count();
        }

        size_t hopBehind(const size_t& end_idx, const size_t& free_idx)
        {
            // startTimer();
            size_t right_idx = free_idx;
            size_t left_idx = end_idx;

            // Cache the value of backInfo_[left_idx] to reduce memory accesses
            size_t currentBackInfo = backInfo_[left_idx];

            if (currentBackInfo != ctgraph::BACKEMPTY)
            {

                backInfo_[right_idx] = currentBackInfo - right_idx + left_idx;
                setHopInfo(left_idx + currentBackInfo, currentBackInfo - right_idx + left_idx);
                setHopInfoInitial(right_idx, right_idx - left_idx, false);
            }
            else
            {
                latestValuePtr_[table_[end_idx].edge_.destination()] = free_idx;
                backInfo_[right_idx] = ctgraph::BACKEMPTY;
                setHopInfoInitial(right_idx, right_idx - left_idx, false);
            }

            backInfo_[left_idx] = right_idx - left_idx;
            control_[free_idx] = control_[end_idx];

            while (!getInitial(left_idx))
            {
                size_t hopInfoValue = getHopInfo(left_idx);
                table_[right_idx] = std::move(table_[left_idx]);
                right_idx = left_idx;
                left_idx = left_idx - hopInfoValue;
            }

            table_[right_idx] = std::move(table_[left_idx]);
            setHopInfoInitial(right_idx, 0, true);
            table_[left_idx] = HashNode();
            backInfo_[left_idx] = ctgraph::BACKEMPTY;
            control_[left_idx] = ctgraph::HOPEMPTY;

            // stopTimer();
            return left_idx;
        }

        size_t findInitialIdx(const size_t& start_idx)
        {
            size_t idx = start_idx;
            while (idx > 0 && !getInitial(idx))
            {
                size_t hop = getHopInfo(idx);
                idx = idx - hop;
            }
            return idx;
        }

        std::optional<size_t> shift(size_t dst_idx, size_t& free_idx)
        {
            size_t now_idx = free_idx - 1;
            size_t continue_hop_idx = capacity_;
            while (free_idx - dst_idx >= maxHopDistance_ && now_idx >= dst_idx)
            {
                if (free_idx - dst_idx <= maxHopDistance_ * ctgraph::HOPSHIFTNUM || free_idx >= continue_hop_idx)
                { // If the distance is short, a single step jump is sufficient; if it has been previously recorded that
                  // continuous jumps are not possible, there is no need for further calculation
                    if (free_idx - now_idx <= maxHopDistance_)
                    { // Within the hop range, it is necessary to determine whether the intermediate values can be
                      // skipped
                        int hop_distance = static_cast<int>(free_idx) - static_cast<int>(now_idx);
                        int hop_after_chage = getHopInfo(now_idx) + hop_distance;
                        if (hop_after_chage <= static_cast<int>(maxHopDistance_))
                        { // still can hop
                            auto& node = table_[now_idx];
                            if (backInfo_[now_idx] == ctgraph::BACKEMPTY)
                            { // If it involves the modification of the latest value, the synchronization update of the
                              // latestValuePtr_ position should be carried out
                                latestValuePtr_[table_[now_idx].edge_.destination()] = free_idx;
                            }
                            else
                            {
                                setHopInfo(now_idx + backInfo_[now_idx], now_idx + backInfo_[now_idx] - free_idx);
                                backInfo_[free_idx] = now_idx + backInfo_[now_idx] - free_idx;
                            }
                            table_[free_idx] = std::move(node);
                            if (getInitial(now_idx))
                            {
                                setHopInfoInitial(free_idx, 0, true);
                            }
                            else
                            {
                                setHopInfoInitial(free_idx, hop_after_chage, false);
                                backInfo_[now_idx - getHopInfo(now_idx)] = hop_after_chage;
                            }

                            table_[now_idx] = HashNode();
                            control_[free_idx] = control_[now_idx];
                            backInfo_[now_idx] = ctgraph::BACKEMPTY;
                            free_idx = now_idx;
                        }
                    }
                }
                else
                { // When the distance is great, connection jumps are employed
                    continue_hop_idx = free_idx;
                    for (int i = 1; i <= maxHopDistance_; i++)
                    {
                        size_t initial_idx = findInitialIdx(free_idx - i);
                        if (initial_idx >= dst_idx)
                        {
                            free_idx = hopBehind(free_idx - i, free_idx);
                            break;
                        }
                    }
                    now_idx = free_idx;
                }
                now_idx--;
            }
            return (free_idx - dst_idx < maxHopDistance_) ? std::optional(free_idx) : std::nullopt;
        }
    };

} // namespace hophash
#endif // HOPSCOTCHHASH_HPP
