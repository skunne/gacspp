#pragma once

#include <experimental/filesystem>
#include <string>
#include <unordered_set>
#include <vector>

#include "IConfigConsumer.hpp"

namespace fs = std::experimental::filesystem;



class CConfigLoader
{
private:
    CConfigLoader() = default;

    fs::path mCurrentDirectory;

public:
    std::unordered_set<std::string> mLoadedFiles;
    std::vector<IConfigConsumer*> mConfigConsumer;

public:
    CConfigLoader(const CConfigLoader&) = delete;
    CConfigLoader& operator=(const CConfigLoader&) = delete;
    CConfigLoader(const CConfigLoader&&) = delete;
    CConfigLoader& operator=(const CConfigLoader&&) = delete;

    static auto GetRef() -> CConfigLoader&;

    bool TryLoadConfig(const fs::path& path);
    bool TryLoadConfig(const json& jsonRoot);
};
