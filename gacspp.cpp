#include <iostream>
#include <algorithm>
#include <cstdint>
#include <cassert>
#include <random>
#include <memory>

#include "CRucio.hpp"
#include "CCloudGCP.hpp"
#include "CScheduleable.hpp"

typedef std::minstd_rand RNGEngineType;

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
private:
    CRucio *mRucio;
    RNGEngineType &mRNGEngine;
    std::normal_distribution<float> mNumFilesRNG;
    std::normal_distribution<double> mFileSizeRNG;
    std::normal_distribution<float> mFileLifetimeRNG;
    const std::uint32_t mTickFreq;

    std::uint64_t CreateFilesAndReplicas(std::uint32_t numFiles, std::uint32_t numReplicasPerFile, std::uint64_t now)
    {
        if(numFiles == 0 || numReplicasPerFile == 0)
            return 0;
        std::uint64_t bytesOfFilesGen = 0;
        for(std::uint32_t i = 0; i < numFiles; ++i)
        {
            const std::uint32_t fileSize = static_cast<std::uint32_t>(mFileSizeRNG(mRNGEngine));
            const std::uint64_t expiresAt = now + static_cast<std::uint64_t>(mFileLifetimeRNG(mRNGEngine));
            /*const SFile &fileObj = */mRucio->CreateFile(fileSize, expiresAt);
            bytesOfFilesGen += fileSize;
            /*
            for(auto *rse : mRSEs)
            {
                mRucio->addReplica(fileObj, rseObj);
                rseObj->increaseReplica(fileObj, 0, fileSize);
            }
            */
        }
        return bytesOfFilesGen;
    }

public:
    CDataGenerator(CRucio *rucio, RNGEngineType &RNGEngine, const std::uint32_t tickFreq)
    :   mRucio(rucio),
        mRNGEngine(RNGEngine),
        mNumFilesRNG(25, 1),
        mFileSizeRNG(1<<24, 4),
        mFileLifetimeRNG(3600*24*5, 1),
        mTickFreq(tickFreq)
    {}

    virtual void OnUpdate(ScheduleType &schedule, std::uint64_t now)
    {
        const std::uint32_t totalFilesToGen = std::max(static_cast<std::uint32_t>(mNumFilesRNG(mRNGEngine)), 1U);

        std::uint32_t numFilesToGen = std::max(static_cast<std::uint32_t>(totalFilesToGen * 0.35f), 1U);
        std::uint64_t totalBytesGen = CreateFilesAndReplicas(numFilesToGen, 1, now);
        std::uint32_t numFilesGen = numFilesToGen;
        std::uint32_t totalReplicasGen = numFilesToGen;

        numFilesToGen = std::max(static_cast<std::uint32_t>(totalFilesToGen * 0.60f), 1U);
        totalBytesGen += CreateFilesAndReplicas(numFilesToGen, 2, now) * 2;
        numFilesGen += numFilesToGen;
        totalReplicasGen += numFilesToGen * 2;

        numFilesToGen = totalFilesToGen - numFilesGen;
        totalBytesGen += CreateFilesAndReplicas(numFilesToGen, 3, now) * 3;
        numFilesGen += numFilesToGen;
        totalReplicasGen += numFilesToGen * 3;

        mNextCallTick = now + mTickFreq;
        schedule.push(this);
    }
};

class CReaper : public CScheduleable
{
private:
    CRucio *mRucio;

public:
	std::uint64_t n = 0;
    CReaper(CRucio *rucio)
    :mRucio(rucio)
    {
        mNextCallTick = 600;
    }

    virtual void OnUpdate(ScheduleType &schedule, std::uint64_t now)
    {
        //std::cout<<mRucio->mFiles.size()<<" files stored at "<<now<<std::endl;
        n += mRucio->RunReaper(now);
        //std::cout<<numDeleted<<" reaper deletions at "<<now<<std::endl;
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
        mActiveTransfers.emplace_back(link, file.GetSize());
    }

    virtual void OnUpdate(ScheduleType &schedule, std::uint64_t now)
    {
		std::uint32_t time_passed = static_cast<std::uint32_t>(now - mLastUpdated);
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
    //std::random_device rngDevice;
    RNGEngineType RNGEngine(42);

    ScheduleType schedule;

    std::unique_ptr<CRucio> rucio(new CRucio);

    std::unique_ptr<CTransferManager> transferMgr(new CTransferManager);
    std::unique_ptr<CDataGenerator> dataGen(new CDataGenerator(rucio.get(), RNGEngine, 50));
    std::unique_ptr<CReaper> reaper(new CReaper(rucio.get()));

    schedule.push(transferMgr.get());
    schedule.push(dataGen.get());
    schedule.push(reaper.get());

    const std::uint64_t MAX_TICK = 3600 * 24 * 21;
    std::uint64_t curTick = 0;
    while(curTick<MAX_TICK && !schedule.empty())
    {
        CScheduleable *element = schedule.top();
        schedule.pop();
        assert(curTick <= element->mNextCallTick);
        curTick = element->mNextCallTick;
        element->OnUpdate(schedule, curTick);
    }
	std::cout << reaper->n << std::endl;
	std::cin >> reaper->n;
}
