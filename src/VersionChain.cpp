#include "VersionChain.hpp"
#include <iostream>
#include <random>
#include <vector>
#include "fixstring.hpp"

int main()
{
    std::vector<int> values = {1, 2, 3, 4, 5};
    std::vector<uint64_t> timestamps;
    std::vector<std::shared_ptr<FixString>> properties;

    for (int i = 0; i < values.size(); i++)
    {
        timestamps.push_back(i * 1000);
        properties.push_back(std::make_shared<FixString>("property_" + std::to_string(i)));
    }

    ctgraph::VersionChain chain;

    for (size_t i = 0; i < values.size(); i++)
    {
        chain.insert(timestamps[i], values[i], properties[i]);
    }

    auto latest = chain.get_latest();
    if (latest)
    {
        std::cout << "Latest version - Timestamp: " << latest->timestamp << ", Value: " << latest->value
                  << ", Property: " << latest->property->data() << std::endl;
    }

    uint64_t test_timestamp = 2000;
    auto version = chain.get_version(test_timestamp);
    if (version)
    {
        std::cout << "Version at timestamp " << test_timestamp << " - Value: " << version->value
                  << ", Property: " << version->property->data() << std::endl;
    }

    std::cout << "\nAll versions:" << std::endl;
    for (const auto& entry : chain.get_all_versions())
    {
        std::cout << "Timestamp: " << entry.timestamp << ", Value: " << entry.value
                  << ", Property: " << entry.property->data() << std::endl;
    }

    return 0;
}
