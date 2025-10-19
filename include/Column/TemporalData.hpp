//
// Created by lwh on 25-2-25.
//

#ifndef TEMPORALDATA_HPP
#define TEMPORALDATA_HPP

#include <iostream>
#include <memory>
#include <vector>


#include "ColumnTypes.hpp"


using Futex = livegraph::Futex;

namespace column
{
    struct Header
    {
        lock_t lock_; // LOCK for every Header
        bitmap_t exist_bitmap_;
    };
    constexpr size_t HeaderSize = sizeof(Header);
    using HeaderPtr = std::shared_ptr<Header>;

    class TemporalData
    {
    private:
        static constexpr size_t INITIAL_VERSION_NUM = 16;
        size_t current_capacity_;
        SequenceNumber_t* seq_;
        bitmap_t* modify_bitmap_;
        next_offset_t* next_offset_;

    public:
        Header header_;
        std::vector<std::vector<offset_t>> value_offset;
        version_count_t version_count_;
        TemporalData() : current_capacity_(INITIAL_VERSION_NUM)
        {
            seq_ = new SequenceNumber_t[INITIAL_VERSION_NUM];
            modify_bitmap_ = new bitmap_t[INITIAL_VERSION_NUM];
            next_offset_ = new next_offset_t[INITIAL_VERSION_NUM];
            std::fill(seq_, seq_ + INITIAL_VERSION_NUM, 0);
            std::fill(modify_bitmap_, modify_bitmap_ + INITIAL_VERSION_NUM, 0);
            std::fill(next_offset_, next_offset_ + INITIAL_VERSION_NUM, 0);
        }

        TemporalData(const TemporalData& other)
        {
            version_count_ = other.version_count_;
            header_ = other.header_;
            current_capacity_ = other.current_capacity_;

            seq_ = new SequenceNumber_t[current_capacity_];
            modify_bitmap_ = new bitmap_t[current_capacity_];
            next_offset_ = new next_offset_t[current_capacity_];

            std::copy(other.seq_, other.seq_ + current_capacity_, seq_);
            std::copy(other.modify_bitmap_, other.modify_bitmap_ + current_capacity_, modify_bitmap_);
            std::copy(other.next_offset_, other.next_offset_ + current_capacity_, next_offset_);


            value_offset = other.value_offset;
        }

        TemporalData(TemporalData&& other) noexcept
        {
            version_count_ = other.version_count_;
            header_ = other.header_;
            current_capacity_ = other.current_capacity_;

            seq_ = other.seq_;
            modify_bitmap_ = other.modify_bitmap_;
            next_offset_ = other.next_offset_;
            value_offset = std::move(other.value_offset);
            other.seq_ = nullptr;
            other.modify_bitmap_ = nullptr;
            other.next_offset_ = nullptr;
            other.version_count_ = 0;
            other.current_capacity_ = 0;
        }

        TemporalData& operator=(const TemporalData& other)
        {
            if (this != &other)
            {
                delete[] seq_;
                delete[] modify_bitmap_;
                delete[] next_offset_;

                version_count_ = other.version_count_;
                header_ = other.header_;
                current_capacity_ = other.current_capacity_;

                seq_ = new SequenceNumber_t[current_capacity_];
                modify_bitmap_ = new bitmap_t[current_capacity_];
                next_offset_ = new next_offset_t[current_capacity_];

                std::copy(other.seq_, other.seq_ + current_capacity_, seq_);
                std::copy(other.modify_bitmap_, other.modify_bitmap_ + current_capacity_, modify_bitmap_);
                std::copy(other.next_offset_, other.next_offset_ + current_capacity_, next_offset_);

                value_offset = other.value_offset;
            }
            return *this;
        }

        TemporalData& operator=(TemporalData&& other) noexcept
        {
            if (this != &other)
            {
                delete[] seq_;
                delete[] modify_bitmap_;
                delete[] next_offset_;

                version_count_ = other.version_count_;
                header_ = other.header_;
                current_capacity_ = other.current_capacity_;

                seq_ = other.seq_;
                modify_bitmap_ = other.modify_bitmap_;
                next_offset_ = other.next_offset_;
                value_offset = std::move(other.value_offset);

                other.seq_ = nullptr;
                other.modify_bitmap_ = nullptr;
                other.next_offset_ = nullptr;
                other.version_count_ = 0;
                other.current_capacity_ = 0;
            }
            return *this;
        }

        ~TemporalData()
        {
            delete[] seq_;
            delete[] modify_bitmap_;
            delete[] next_offset_;
        }

        void resize(size_t new_capacity)
        {
            if (new_capacity <= current_capacity_)
                return;

            SequenceNumber_t* new_seq = new SequenceNumber_t[new_capacity];
            bitmap_t* new_modify_bitmap = new bitmap_t[new_capacity];
            next_offset_t* new_next_offset = new next_offset_t[new_capacity];

            std::copy(seq_, seq_ + current_capacity_, new_seq);
            std::copy(modify_bitmap_, modify_bitmap_ + current_capacity_, new_modify_bitmap);
            std::copy(next_offset_, next_offset_ + current_capacity_, new_next_offset);
            std::fill(new_seq + current_capacity_, new_seq + new_capacity, 0);
            std::fill(new_modify_bitmap + current_capacity_, new_modify_bitmap + new_capacity, 0);
            std::fill(new_next_offset + current_capacity_, new_next_offset + new_capacity, 0);
            delete[] seq_;
            delete[] modify_bitmap_;
            delete[] next_offset_;
            seq_ = new_seq;
            modify_bitmap_ = new_modify_bitmap;
            next_offset_ = new_next_offset;
            current_capacity_ = new_capacity;
        }

        void ensure_capacity(size_t required_version)
        {
            if (required_version >= current_capacity_)
            {
                size_t new_capacity = current_capacity_ * 2;
                while (new_capacity <= required_version)
                {
                    new_capacity *= 2;
                }
                resize(new_capacity);
            }
        }

        SequenceNumber_t get_seq(size_t version) const
        {
            if (version >= current_capacity_)
                return 0;
            return seq_[version];
        }

        void set_seq(size_t version, SequenceNumber_t value)
        {
            ensure_capacity(version);
            seq_[version] = value;
        }

        bitmap_t get_modify_bitmap(size_t version) const
        {
            if (version >= current_capacity_)
                return 0;
            return modify_bitmap_[version];
        }

        void set_modify_bitmap(size_t version, bitmap_t value)
        {
            ensure_capacity(version);
            modify_bitmap_[version] = value;
        }

        next_offset_t get_next_offset(size_t version) const
        {
            if (version >= current_capacity_)
                return 0;
            return next_offset_[version];
        }

        void set_next_offset(size_t version, next_offset_t value)
        {
            ensure_capacity(version);
            next_offset_[version] = value;
        }
    };
} // namespace column


#endif // TEMPORALDATA_HPP
