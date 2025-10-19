#include <fstream>
#include <gflags/gflags.h>
#include <iostream>
#include <map>
#include <omp.h>
#include <random>
#include <regex>
#include <set>
#include <sstream>
#include <sys/resource.h>
#include <unordered_set>
#include <vector>


void processVertices(const std::vector<std::pair<uint64_t, uint64_t>>& edges, const std::string& vertexFile)
{
    std::set<uint64_t> vertices;
    for (const auto& edge : edges)
    {
        vertices.insert(edge.first);
        vertices.insert(edge.second);
    }
    std::ofstream vFile(vertexFile);
    for (const auto& vertex : vertices)
    {
        vFile << vertex << std::endl;
    }
    vFile.close();
}

void processCypher(const std::string& inputFile)
{
    std::ifstream file(inputFile);
    std::string line;
    std::set<std::string> uniquePatterns;
    std::map<std::string, std::ofstream*> patternFiles;

    int lineCount = 0;
    while (std::getline(file, line))
    {
        lineCount++;
        std::regex createPattern("^\\s*CREATE\\s*\\([^:]*:([^\\s{]*)(?::([^\\s{]*))?");
        std::regex matchPattern(
            R"(^\s*MATCH\s*\(\w*:(\w+)\s*\{[^}]*\}\),\s*\(\w*:(\w+)\s*\{[^}]*\}\)\s*CREATE\s*\(\w*\)-\[:(\w+)\{[^}]*\}\]->)");

        std::smatch matches;
        std::string pattern;
        if (std::regex_search(line, matches, createPattern))
        {
            pattern = "CREATE";
            for (size_t i = 1; i < matches.size(); ++i)
            {
                if (matches[i].matched)
                {
                    pattern += "_" + matches[i].str();
                }
            }
        }
        else if (std::regex_search(line, matches, matchPattern))
        {
            pattern = "MATCH";
            if (matches[1].matched)
            {
                pattern += "_" + matches[1].str();
            }
            if (matches[2].matched)
            {
                pattern += "_" + matches[2].str();
            }
            if (matches[3].matched)
            {
                pattern += "_" + matches[3].str();
            }
        }

        if (!pattern.empty())
        {
            uniquePatterns.insert(pattern);
            if (patternFiles.find(pattern) == patternFiles.end())
            {
                std::string filename = "/home/lwh/dataset/ldbc/" + pattern + ".txt";
                patternFiles[pattern] = new std::ofstream(filename);
            }
            *patternFiles[pattern] << line << std::endl;
        }
    }
    for (auto& pair : patternFiles)
    {
        pair.second->close();
        delete pair.second;
    }
}

int main(int argc, char** argv)
{
    printf("Size of size_t: %zu bytes\n", sizeof(size_t));
    printf("Maximum value of size_t: %zu\n", (size_t)-1);
    processCypher("/home/lwh/dataset/ldbc/ldbc.cypher");
    return 0;
}
