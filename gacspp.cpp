#include <iostream>
#include <cstdint>

#include "CScheduleable.hpp"


struct SLink
{
    std::uint32_t mBandwidth;
    std::uint64_t mUsedTraffic = 0;

    SLink(std::uint32_t bandwidth)
    :mBandwidth(bandwidth)
    {}
};

class CDataGenerator : public CScheduleable
{
public:
    virtual void OnUpdate(ScheduleType &schedule, std::uint64_t now)
    {
        //std::cout<<mNextCallTick<<" DataGen\n";
        mNextCallTick = now + 50;
        schedule.push(this);
    }
};

class CReaper : public CScheduleable
{
private:
    CRucio *mRucio;

public:
    CReaper(CRucio *rucio)
    :mRucio(rucio)
    {
        mNextCallTick = 600;
    }

    virtual void OnUpdate(ScheduleType &schedule, std::uint64_t now)
    {
        std::cout<<mRucio->mFiles.size()<<" files stored at "<<now<<std::endl;
        auto numDeleted = mRucio->RunReaper(now);
        std::cout<<numDeleted<<" reaper deletions at "<<now<<std::endl;
        mNextCallTick = now + 600;
        schedule.push(this);
    }
};

class CTransferManager : public CScheduleable
{
private:
    struct STransfer
    {
        std::uint32_t &mBandwidth;
        std::uint64_t &mUsedTraffic;
        const std::uint32_t mFinalSize;
        std::uint32_t mCurSize = 0;

        STransfer(SLink &link, std::uint32_t finalSize)
            :mBandwidth(link.mBandwidth),
            mUsedTraffic(link.mUsedTraffic),
            mFinalSize(finalSize)
        {}
        //STransfer(const STransfer &other) = delete;
        STransfer &operator=(const STransfer &other) = delete;
    };

    std::uint64_t mLastUpdated = 0;
    std::vector<STransfer> mActiveTransfers;
    //std::stack<std::size_t, std::vector<std::size_t>> mFinishedIndices;

public:
    CTransferManager()
    {
        mActiveTransfers.reserve(1024);
    }
    void CreateTransfer(SLink &link, SFile &file)
    {
        mActiveTransfers.emplace_back(link, file.mSize);
    }

    virtual void OnUpdate(ScheduleType &schedule, std::uint64_t now)
    {
        std::uint32_t time_passed = now - mLastUpdated;
        mLastUpdated = now;
        for(STransfer &curTransfer : mActiveTransfers)
        {
            const auto finalSize = curTransfer.mFinalSize;
            auto &curSize = curTransfer.mCurSize;
            std::uint32_t transferred = curTransfer.mBandwidth * time_passed;
            curSize += transferred;

            if(curSize >= finalSize)
            {
                transferred -= (curSize - finalSize);
                curSize = finalSize;
                //transfer.state = STransfer::SUCCESS;
                //mFinishedIndices.push(i);
            }

            curTransfer.mUsedTraffic += transferred;
            //curTransfer.mDestRSE.increase_replica(transferred);
        }
        //howto: remove finished transfers
        //1. stack with indices of finished transfer; clean up mActiveTransfers after iteration
        //2. second vector gets build with only still active transfers in it
        //std::cout<<mNextCallTick<<" Transferer\n";
        mNextCallTick = now + 30;
        schedule.push(this);
    }
};

int main()
{
    ScheduleType schedule;

    CRucio *rucio = new CRucio;

    CTransferManager *transferMgr = new CTransferManager;
    CDataGenerator *datagen = new CDataGenerator;
    CReaper *reaper = new CReaper(rucio);

    schedule.push(transferMgr);
    schedule.push(datagen);
    schedule.push(reaper);

    for(int j=0;j<5;++j)
    {
        for(int i=0;i<500000;i+=3)
        {
            rucio->CreateFile(i*j, 1024);
        }
    }

    const std::uint64_t MAX_TICK = 10000;//3600 * 24 * 60;
    std::uint64_t curTick = 0;
    while(curTick<MAX_TICK && !schedule.empty())
    {
        CScheduleable *element = schedule.top();
        schedule.pop();
        curTick = element->mNextCallTick;
        element->OnUpdate(schedule, curTick);
    }
}
