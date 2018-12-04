#pragma once

#include "COutput.hpp"
#include "IBaseSim.hpp"


class CSimpleSim : public IBaseSim
{
public:
    void SetupDefaults() override;
};
