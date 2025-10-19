//
// Created by lwh on 25-1-3.
//

#ifndef NEIGHBORS_HPP
#define NEIGHBORS_HPP
#include <flags.hpp>

#include "hopscotch/hopscotchhash.hpp"

namespace ctgraph
{
    /**
     * The n-to-n edges are represented using an Array combined with hopscotch hashing
     */
    class NewEdgeAH;

    using NeighBors = NewEdgeAH;

    class NewEdgeAH
    {
    private:
        bool full_; // Check if the array is full
        bool migrate_; // Check if migration is needed
        Edge** dst_;
        std::vector<uint8_t> temp_cnt_;
        int cnt_;
        size_t property_size_;
        hophash::HopscotchHashTable* list_;

    public:
        NewEdgeAH() : full_(false), migrate_(false), dst_(nullptr), cnt_(0), property_size_(0)
        {
            if (FLAGS_reserve_node > 0)
            {
                dst_ = new Edge*[FLAGS_reserve_node];
                for (int i = 0; i < FLAGS_reserve_node; ++i)
                {
                    dst_[i] = new Edge[FLAGS_reserve_node]; // Each version also only has FLAGS_reserve_node instances
                }
                temp_cnt_.assign(FLAGS_reserve_node, 0);
                // list_ = new hophash::HopscotchHashTable();
            }
        }

        ~NewEdgeAH()
        {
            if (FLAGS_reserve_node > 0)
            {
                for (int i = 0; i < FLAGS_reserve_node; ++i)
                {
                    delete[] dst_[i];
                }
                delete[] dst_;
            }
            if (full_)
            {
                delete list_;
            }
        }

        // void put_edge(VertexId_t dst, SequenceNumber_t seq,
        //                Marker_t marker, EdgeProperty_t &property) {
        void put_edge(Edge& edge)
        {
            // EdgeProperty_t new_property_view = put_property(property);
            // Edge edge(dst, seq, marker, new_property_view);
            if (!full_)
            {
                if (migrate_)
                {
                    list_ = new hophash::HopscotchHashTable();
                    for (int i = 0; i < FLAGS_reserve_node; i++)
                    {
                        for (int j = 0; j <= temp_cnt_[i]; j++)
                        {
                            Edge edge_old = Edge(dst_[i][j].destination(), dst_[i][j].sequence(), dst_[i][j].marker(),
                                                 &dst_[i][j].property());
                            list_->insertWithTs(edge_old);
                        }
                    }
                    list_->insertWithTs(edge);
                    full_ = true;
                }
                else
                {
                    bool find = false;
                    for (int i = 0; i < cnt_; i++)
                    {
                        if (dst_[i][0].destination() == edge.destination())
                        {
                            temp_cnt_[i]++;
                            dst_[i][temp_cnt_[i]] = edge;
                            if (temp_cnt_[i] == FLAGS_reserve_node - 1)
                            {
                                migrate_ = true;
                            }
                            find = true;
                            break;
                        }
                    }
                    if (!find)
                    {
                        dst_[cnt_++][0] = edge;
                        if (cnt_ == FLAGS_reserve_node)
                        {
                            migrate_ = true;
                        }
                    }
                }
            }
            else
            {
                list_->insertWithTs(edge);
            }
            property_size_ += edge.property().size();
        }

        Status get(VertexId_t dst, EdgeProperty_t*& property)
        {
            // std::cout << " properties=" << properties << std::endl;
            if (!full_)
            {
                int i = 0;
                while (i < this->cnt_)
                {
                    if (this->dst_[i][0].destination() == dst)
                    {
                        if (this->dst_[i][temp_cnt_[i]].marker())
                            return kDelete;
                        property = &this->dst_[i][temp_cnt_[i]].property();
                        return kOk;
                    }
                    i++;
                }
                return kNotFound;
            }
            else
            {
                return list_->get_latest(dst, property);
            }
        }

        Status get_withTs(VertexId_t dst, EdgeProperty_t*& property, SequenceNumber_t ts)
        {
            // std::cout << " properties=" << properties << std::endl;
            if (!full_)
            {
                int i = 0;
                while (i < this->cnt_)
                {
                    if (this->dst_[i][0].destination() == dst)
                    {
                        for (int j = 0; j < temp_cnt_[i]; j++)
                        {
                            if (this->dst_[i][j].sequence() <= ts && ts < this->dst_[i][j + 1].sequence())
                            {
                                if (this->dst_[i][j].marker())
                                    return kDelete;
                                property = &this->dst_[i][j].property();
                                return kOk;
                            }
                        }
                        if (this->dst_[i][temp_cnt_[i]].sequence() <= ts)
                        {
                            if (this->dst_[i][temp_cnt_[i]].marker())
                                return kDelete;
                            property = &this->dst_[i][temp_cnt_[i]].property();
                            return kOk;
                        }
                        return kNotFound;
                    }
                    i++;
                }
                return kNotFound;
            }
            else
            {
                return list_->getVersionInRange(dst, ts, property);
            }
        }

        bool get_edges(std::vector<Edge>& edges)
        {
            if (!full_)
            {
                int i = 0;
                while (i < this->cnt_)
                {
                    edges.push_back(this->dst_[i][temp_cnt_[i]]);
                    i++;
                }
                return true;
            }
            else
            {
                return list_->get_edges(edges);
            }
        }

        bool get_edges_withTs(std::vector<Edge>& edges, SequenceNumber_t ts)
        {
            if (!full_)
            {
                int i = 0;
                while (i < this->cnt_)
                {
                    bool find = false;
                    for (int j = 0; j < temp_cnt_[i]; j++)
                    {
                        if (this->dst_[i][j].sequence() <= ts && ts < this->dst_[i][j + 1].sequence())
                        {
                            edges.push_back(this->dst_[i][j]);
                            find = true;
                            break;
                        }
                    }
                    if (!find && this->dst_[i][temp_cnt_[i]].sequence() <= ts)
                    {
                        edges.push_back(this->dst_[i][temp_cnt_[i]]);
                    }
                    i++;
                }
                return true;
            }
            else
            {
                return list_->get_edges_withTs(edges, ts);
            }
        }
    };
} // namespace ctgraph

#endif // NEIGHBORS_HPP
