#pragma once

#include <string>
#include <unordered_set>
#include <vector>

#include "json.hpp"



class IConfigConsumer
{
public:
    virtual bool TryConsumeConfig(const nlohmann::json::const_iterator& json) = 0;
};



class CConfigLoader
{
private:
    CConfigLoader() = default;

public:
    std::unordered_set<std::string> mLoadedFiles;
    std::vector<IConfigConsumer*> mConfigConsumer;

public:
    CConfigLoader(const CConfigLoader&) = delete;
    CConfigLoader& operator=(const CConfigLoader&) = delete;
    CConfigLoader(const CConfigLoader&&) = delete;
    CConfigLoader& operator=(const CConfigLoader&&) = delete;

    static auto GetRef() -> CConfigLoader&;

    bool TryLoadConfig(const std::string& path);
    bool TryLoadConfig(const nlohmann::json& jsonRoot);
};
