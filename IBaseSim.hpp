#pragma once

#include <cstdint>
#include <memory>
#include <random>
#include <vector>

#include "CScheduleable.hpp"

class IBaseCloud;
class CRucio;

class IBaseSim
{
public:
    typedef std::minstd_rand RNGEngineType;
    typedef std::uint64_t TickType;

    inline static auto GetCurrentTick() -> TickType
    {return IBaseSim::mCurrentTick;}

private:
    static TickType mCurrentTick;

protected:
    ScheduleType mSchedule;

public:
    //std::random_device rngDevice;
    RNGEngineType mRNGEngine {42};

    //rucio and clouds
    std::unique_ptr<CRucio> mRucio;
    std::vector<std::unique_ptr<IBaseCloud>> mClouds;


    virtual void SetupDefaults() = 0;
    virtual void Run(const TickType maxTick);
};
