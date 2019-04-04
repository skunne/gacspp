#include <fstream>
#include <iostream>

#include "constants.h"
#include "CConfigLoader.hpp"


auto CConfigLoader::GetRef() -> CConfigLoader&
{
    static CConfigLoader mInstance;
    return mInstance;
}

bool CConfigLoader::TryLoadConfig(const nlohmann::json& jsonRoot)
{
    if(mConfigConsumer.empty())
        std::cout << "No config consumers registered" << std::endl;

    for(nlohmann::json::const_iterator jsonRootIt=jsonRoot.cbegin(); jsonRootIt!=jsonRoot.cend(); ++jsonRootIt)
    {
        bool wasConsumed = false;
        auto resultIt = jsonRootIt.value().find(JSON_FILE_IMPORT_KEY);
        if(jsonRootIt.value().is_object() && resultIt != jsonRootIt.value().end())
        {
            if(!resultIt.value().is_string())
            {
                std::cout << "Expected value of _file_ field to be of type string. Ignoring entry:" << std::endl;
                std::cout << resultIt->dump(JSON_DUMP_SPACES) << std::endl;
                continue;
            }

            wasConsumed = TryLoadConfig(resultIt.value().get<std::string>());
        }
        else
        {
            for(IConfigConsumer* consumer : mConfigConsumer)
                wasConsumed = wasConsumed || consumer->TryConsumeConfig(jsonRootIt);
        }

        if(!wasConsumed)
        {
            std::cout << "Didnt consume json entry:" << std::endl;
            std::cout << jsonRootIt->dump(JSON_DUMP_SPACES) << std::endl;
        }
    }

    return true;
}

bool CConfigLoader::TryLoadConfig(const std::string& path)
{
    std::pair<std::unordered_set<std::string>::iterator, bool> insertResult = mLoadedFiles.insert(path);
    if(!insertResult.second)
    {
        std::cout << "Preventing file from being loaded twice: " << path << std::endl;
        return false;
    }

    nlohmann::json jsonRoot;

    {
        std::ifstream configFile(path);
        if(!configFile)
        {
            std::cout << "Unable to load config: " << path << std::endl;
            return false;
        }
        configFile >> jsonRoot;
    }

    return TryLoadConfig(jsonRoot);
}
