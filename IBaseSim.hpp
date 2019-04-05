#pragma once

#include <memory>
#include <random>
#include <vector>

#include "constants.h"

#include "CScheduleable.hpp"

class IBaseCloud;
class CRucio;



class IBaseSim
{
public:
    IBaseSim();
    virtual ~IBaseSim();

    //std::random_device rngDevice;
    RNGEngineType mRNGEngine {42};

    //rucio and clouds
    std::unique_ptr<CRucio> mRucio;
    std::vector<std::unique_ptr<IBaseCloud>> mClouds;

    virtual void SetupDefaults() = 0;
    virtual void Run(const TickType maxTick);

protected:
    ScheduleType mSchedule;

private:
    TickType mCurrentTick;
};
