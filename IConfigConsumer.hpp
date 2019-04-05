#pragma once

#include "json_fwd.hpp"

using json = nlohmann::json;



class IConfigConsumer
{
public:
    virtual bool TryConsumeConfig(const json& json) = 0;
};
