#include <cassert>
#include <cstdint>
#include <memory>

#include "IBaseSim.hpp"

IBaseSim::TickType IBaseSim::mCurrentTick = 0;

void IBaseSim::Run(const TickType maxTick)
{
    IBaseSim::mCurrentTick = 0;
    while(IBaseSim::mCurrentTick<maxTick && !mSchedule.empty())
    {
        std::shared_ptr<CScheduleable> element = mSchedule.top();
        mSchedule.pop();
        assert(IBaseSim::mCurrentTick <= element->mNextCallTick);
        IBaseSim::mCurrentTick = element->mNextCallTick;
        element->OnUpdate(IBaseSim::mCurrentTick);
        if(element->mNextCallTick > IBaseSim::mCurrentTick)
            mSchedule.push(element);
    }
}
