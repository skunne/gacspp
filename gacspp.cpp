#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <sstream>

#include "constants.h"
#include "CRucio.hpp"
#include "CCloudGCP.hpp"
#include "CScheduleable.hpp"

typedef std::minstd_rand RNGEngineType;

class CDataGenerator : public CScheduleable
{
private:
    CRucio* mRucio;
    RNGEngineType* mRNGEngine;
    std::normal_distribution<float> mNumFilesRNG {20, 1};
    std::normal_distribution<double> mFileSizeRNG {1, 0.5};
    std::normal_distribution<float> mFileLifetimeRNG {6, 1};
    const std::uint32_t mTickFreq;
    inline std::uint32_t GetRandomFileSize()
    {
        const double min = ONE_GiB / 16;
        const double max = static_cast<double>(std::numeric_limits<std::uint32_t>::max());
        const double val = GiB_TO_BYTES(std::abs(mFileSizeRNG(*mRNGEngine)));
        return static_cast<std::uint32_t>(std::clamp(val, min, max));
    }
    inline std::uint32_t GetRandomNumFilesToGenerate()
    {
        return static_cast<std::uint32_t>( std::max(1.f, mNumFilesRNG(*mRNGEngine)) );
    }
    inline std::uint64_t GetRandomLifeTime()
    {
        float val = DAYS_TO_SECONDS(std::abs(mFileLifetimeRNG(*mRNGEngine)));
        return static_cast<std::uint64_t>( std::max(float(SECONDS_PER_DAY), val) );
    }

    std::uint64_t CreateFilesAndReplicas(const std::uint32_t numFiles, const std::uint32_t numReplicasPerFile, const std::uint64_t now)
    {
        if(numFiles == 0 || numReplicasPerFile == 0)
            return 0;

        const std::uint32_t numStorageElements = static_cast<std::uint32_t>( mStorageElements.size() );

        assert(numReplicasPerFile <= numStorageElements);

        std::uniform_int_distribution<std::uint32_t> rngSampler(0, numStorageElements);
        std::uint64_t bytesOfFilesGen = 0;
        for(std::uint32_t i = 0; i < numFiles; ++i)
        {
            const std::uint32_t fileSize = GetRandomFileSize();
            const std::uint64_t expiresAt = now + GetRandomLifeTime();

            SFile* const file = mRucio->CreateFile(fileSize, expiresAt);
            bytesOfFilesGen += fileSize;

            auto reverseRSEIt = mStorageElements.rbegin();
            //numReplicasPerFile <= numStorageElements !
            for(std::uint32_t numCreated = 0; numCreated<numReplicasPerFile; ++numCreated)
            {
                auto selectedElementIt = mStorageElements.begin() + (rngSampler(*mRNGEngine) % (numStorageElements - numCreated));
                (*selectedElementIt)->CreateReplica(file)->Increase(fileSize, now);
                std::iter_swap(selectedElementIt, reverseRSEIt);
				++reverseRSEIt;
            }
        }
        return bytesOfFilesGen;
    }

public:
    std::vector<CStorageElement*> mStorageElements;
    CDataGenerator(CRucio* rucio, RNGEngineType* RNGEngine, const std::uint32_t tickFreq)
    :   mRucio(rucio),
        mRNGEngine(RNGEngine),
        mTickFreq(tickFreq)
    {}

    void OnUpdate(ScheduleType& schedule, std::uint64_t now) final
    {
        auto curRealtime = std::chrono::high_resolution_clock::now();
        const std::uint32_t totalFilesToGen = GetRandomNumFilesToGenerate();
/*
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
*/

        std::uint32_t numFilesToGen = std::max(static_cast<std::uint32_t>(totalFilesToGen * 0.4f), 1U);
        CreateFilesAndReplicas(numFilesToGen, 1, now);

        numFilesToGen = totalFilesToGen - numFilesToGen;
        CreateFilesAndReplicas(numFilesToGen, 2, now);

        mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
        mNextCallTick = now + mTickFreq;
        schedule.push(this);
    }
};

class CReaper : public CScheduleable
{
private:
    CRucio *mRucio;

public:
    CReaper(CRucio *rucio)
        : mRucio(rucio)
    {
        mNextCallTick = 600;
    }

    void OnUpdate(ScheduleType& schedule, std::uint64_t now) final
    {
        auto curRealtime = std::chrono::high_resolution_clock::now();
        mRucio->RunReaper(now);

        mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
        mNextCallTick = now + 600;
        schedule.push(this);
    }
};

class CBillingGenerator : public CScheduleable
{
private:
    gcp::CCloud* mCloud;

public:
    CBillingGenerator(gcp::CCloud* cloud)
        : mCloud(cloud)
    {
        mNextCallTick = SECONDS_PER_MONTH;
    }

    void OnUpdate(ScheduleType& schedule, std::uint64_t now) final
    {
        std::cout<<"Billing for Month "<<(now/SECONDS_PER_MONTH)<<":\n";
        auto res = mCloud->ProcessBilling(now);
        std::cout<<"\tStorage: "<<res.first<<std::endl;
        std::cout<<"\tNetwork: "<<res.second<<std::endl;

        mNextCallTick = now + SECONDS_PER_MONTH;
        schedule.push(this);
    }
};

class CTransferManager : public CScheduleable
{
private:
    struct STransfer
    {
        CLinkSelector* mLinkSelector;
        SReplica* mDstReplica;
        std::uint64_t mStartTick;

        STransfer(CLinkSelector* linkSelector, SReplica* dstReplica, std::uint64_t startTick)
            : mLinkSelector(linkSelector),
              mDstReplica(dstReplica),
              mStartTick(startTick)
        {}
    };



    std::uint64_t mLastUpdated = 0;
    std::vector<STransfer> mActiveTransfers;
    void DeleteTransferUnordered(std::size_t idx)
    {
        auto& transfer = mActiveTransfers[idx];
        do {
            --(transfer.mLinkSelector->mNumActiveTransfers);
            transfer = std::move(mActiveTransfers.back());
            mActiveTransfers.pop_back();
        } while (transfer.mDstReplica == nullptr && !mActiveTransfers.empty());

        if (!mActiveTransfers.empty())
            transfer.mDstReplica->mTransferRef = &(transfer.mDstReplica);
    }
public:
    std::uint32_t mNumCompletedTransfers = 0;
    std::uint64_t mSummedTransferDuration = 0;
    CTransferManager()
    {
        mActiveTransfers.reserve(1024);
    }

    void CreateTransfer(CLinkSelector* linkSelector, SReplica* dstReplica, std::uint64_t now)
    {
        assert(mActiveTransfers.size() < mActiveTransfers.capacity());
        linkSelector->mNumActiveTransfers += 1;
        mActiveTransfers.emplace_back(linkSelector, dstReplica, now);
        dstReplica->mTransferRef = &(mActiveTransfers.back().mDstReplica);
    }
    void CreateTransfer(CStorageElement* srcStorageElement, SReplica* dstReplica, std::uint64_t now)
    {
        CStorageElement* dstStorageElement = dstReplica->GetStorageElement();
        CLinkSelector* linkSelector = srcStorageElement->GetSite()->GetLinkSelector(dstStorageElement->GetSite());
        CreateTransfer(linkSelector, dstReplica, now);
    }

    void OnUpdate(ScheduleType& schedule, std::uint64_t now) final
    {
        auto curRealtime = std::chrono::high_resolution_clock::now();
		const std::uint32_t timeDiff = static_cast<std::uint32_t>(now - mLastUpdated);
        mLastUpdated = now;

        std::size_t idx = 0;
        while (idx < mActiveTransfers.size())
        {
            auto& transfer = mActiveTransfers[idx];
            SReplica* const dstReplica = transfer.mDstReplica;
            if (dstReplica == nullptr)
            {
                DeleteTransferUnordered(idx);
                continue; // handle same idx again
            }

            CLinkSelector* const linkSelector = transfer.mLinkSelector;
            const double sharedBandwidth = linkSelector->mBandwidth / static_cast<double>(linkSelector->mNumActiveTransfers);
            std::uint32_t amount = static_cast<std::uint32_t>(sharedBandwidth * timeDiff);
            amount = dstReplica->Increase(amount, now);
            linkSelector->mUsedTraffic += amount;

            if(dstReplica->IsComplete())
            {
                ++mNumCompletedTransfers;
                mSummedTransferDuration += now - transfer.mStartTick;
                transfer.mDstReplica->mTransferRef = nullptr;
                DeleteTransferUnordered(idx);
                continue; // handle same idx again
            }
            ++idx;
        }

        mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
        mNextCallTick = now + 30;
        schedule.push(this);
    }
    inline auto GetNumActiveTransfers() const -> std::size_t
    {return mActiveTransfers.size();}
};

class CTransferNumberGenerator
{
    RNGEngineType* mRNGEngine;
    double mSoftmaxScale = 15;
    double mSoftmaxOffset = 600;
    double mAlpha = 1.0/30.0 * PI/180.0 * 0.075;

public:
    std::normal_distribution<double> mSoftmaxRNG {0, 1};
    std::normal_distribution<double> mPeakinessRNG {1.05, 0.04};

    CTransferNumberGenerator(RNGEngineType* rngEngine, double softmaxScale, double softmaxOffset, std::uint32_t samplingFreq, double baseFreq)
    : mRNGEngine(rngEngine),
      mSoftmaxScale(softmaxScale),
      mSoftmaxOffset(softmaxOffset),
      mAlpha(1.0/samplingFreq * PI/180.0 * baseFreq)
    {}

    inline std::uint32_t GetNumToCreate(std::uint64_t now, std::uint32_t numActive)
    {
        const double softmax = (std::cos(now * mAlpha) * mSoftmaxScale + mSoftmaxOffset) * (1.0 + mSoftmaxRNG(*mRNGEngine) * 0.02);
        const double diffSoftmaxActive = softmax - numActive;
        if(diffSoftmaxActive < 0.5)
            return 0;
        return static_cast<std::uint32_t>(std::pow(diffSoftmaxActive, abs(mPeakinessRNG(*mRNGEngine))));
    }
};

class CTransferGenerator : public CScheduleable
{
private:
    RNGEngineType* mRNGEngine;
    CTransferManager* mTransferMgr;
    CTransferNumberGenerator* mTransferNumberGen;
    std::uint32_t mDelay;

public:
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<CStorageElement*> mDstStorageElements;

    CTransferGenerator(RNGEngineType* rngEngine, CTransferManager* transferMgr, CTransferNumberGenerator* transferNumberGen, std::uint32_t delay)
        : mRNGEngine(rngEngine),
		  mTransferMgr(transferMgr),
          mTransferNumberGen(transferNumberGen),
          mDelay(delay)
    {}

    void OnUpdate(ScheduleType& schedule, std::uint64_t now) final
    {
        auto curRealtime = std::chrono::high_resolution_clock::now();
        assert(mSrcStorageElements.size() > 0);
        const std::uint32_t numActive = static_cast<std::uint32_t>(mTransferMgr->GetNumActiveTransfers());
        const std::uint32_t numToCreate = mTransferNumberGen->GetNumToCreate(now, numActive);
        const std::uint32_t numToCreatePerRSE = 1 + static_cast<std::uint32_t>( numToCreate/static_cast<double>(mSrcStorageElements.size()) );
        std::uint32_t totalTransfersCreated = 0;
        std::uniform_int_distribution<std::size_t> dstStorageElementRndChooser(0, mDstStorageElements.size()-1);
        for(auto& srcStorageElement : mSrcStorageElements)
        {
            std::uint32_t numCreated = 0;
            std::size_t numSrcReplicas = srcStorageElement->mReplicas.size();
            std::uniform_int_distribution<std::size_t> rngSampler(0, numSrcReplicas);
            while(numSrcReplicas > 0 && numCreated <= numToCreatePerRSE)
            {
                std::size_t idx = rngSampler(*mRNGEngine) % numSrcReplicas;
                --numSrcReplicas;

                auto& curReplica = srcStorageElement->mReplicas[idx];
                if(curReplica->IsComplete())
                {
                    CStorageElement* const dstStorageElement = mDstStorageElements[dstStorageElementRndChooser(*mRNGEngine)];
                    SReplica* const newReplica = dstStorageElement->CreateReplica(curReplica->GetFile());
                    if(newReplica != nullptr)
                    {
                        mTransferMgr->CreateTransfer(srcStorageElement, newReplica, now);
                        ++numCreated;
                    }
                }
                auto& lastReplica = srcStorageElement->mReplicas[numSrcReplicas];
                std::swap(curReplica->mIndexAtStorageElement, lastReplica->mIndexAtStorageElement);
                std::swap(curReplica, lastReplica);
            }
            totalTransfersCreated += numCreated;
        }

        //std::cout<<"["<<now<<"]\nnumActive: "<<numActive<<"\nnumToCreate: "<<numToCreate<<"\ntotalCreated: "<<totalTransfersCreated<<std::endl;

        mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
        mNextCallTick = now + mDelay;
        schedule.push(this);
    }
};

class CHearbeat : public CScheduleable
{
private:
    CRucio* mRucio;
    CTransferManager* mG2CTransferMgr;
    CTransferManager* mC2GTransferMgr;
    std::uint32_t mDelay;

    std::chrono::high_resolution_clock::time_point mTimeLastUpdate;

public:
    std::unordered_map<std::string, std::chrono::duration<double>*> mProccessDurations;

    CHearbeat(CRucio* rucio, CTransferManager* g2cTransferMgr, CTransferManager* c2gTransferMgr, std::uint32_t delay)
        : mRucio(rucio),
          mG2CTransferMgr(g2cTransferMgr),
          mC2GTransferMgr(c2gTransferMgr),
          mDelay(delay)
    {
        mTimeLastUpdate = std::chrono::high_resolution_clock::now();
    }


    void OnUpdate(ScheduleType& schedule, std::uint64_t now) final
    {
        auto curRealtime = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> timeDiff = curRealtime - mTimeLastUpdate;
        mUpdateDurationSummed += timeDiff;
        mTimeLastUpdate = curRealtime;

        std::stringstream statusOutput;
        statusOutput << std::fixed << std::setprecision(2);

        statusOutput << "[" << std::setw(6) << static_cast<std::uint32_t>(now / 1000) << "k]: ";
        statusOutput << "Runtime: " << mUpdateDurationSummed.count() << "s; ";
        statusOutput << "numFiles: " << mRucio->mFiles.size() / 1000.0 << "k; ";
        statusOutput << "activeTransfers: " << mG2CTransferMgr->GetNumActiveTransfers() << " + " << mC2GTransferMgr->GetNumActiveTransfers() << "; ";
        statusOutput << "CompletedTransfers: " << mG2CTransferMgr->mNumCompletedTransfers << " + " << mC2GTransferMgr->mNumCompletedTransfers << "; ";
        statusOutput << "AvgTransferDuration: " << (mG2CTransferMgr->mSummedTransferDuration / mC2GTransferMgr->mNumCompletedTransfers) << "s\n";
        mG2CTransferMgr->mNumCompletedTransfers = 0;
        mG2CTransferMgr->mSummedTransferDuration = 0;
        mC2GTransferMgr->mNumCompletedTransfers = 0;
        mC2GTransferMgr->mSummedTransferDuration = 0;

		std::size_t maxW = 0;
		for (auto it : mProccessDurations)
			if (it.first.size() > maxW)
				maxW = it.first.size();

        statusOutput << "  " << std::setw(maxW) << "Duration" << ": " << std::setw(7) << timeDiff.count() << "s\n";
        for(auto it : mProccessDurations)
        {
            statusOutput << "  " << std::setw(maxW) << it.first;
            statusOutput << ": " << std::setw(7) << it.second->count();
            statusOutput << "s ("<< std::setw(5) << (it.second->count() / timeDiff.count()) * 100 << "%)\n";
            *(it.second) = std::chrono::duration<double>::zero();
        }
        std::cout << statusOutput.str() << std::endl;

        mNextCallTick = now + mDelay;
        schedule.push(this);
    }
};

int main()
{
    //std::random_device rngDevice;
    std::unique_ptr<RNGEngineType> RNGEngine(new RNGEngineType(42));

    const std::uint32_t transferGenerationDelay = 50;

    ScheduleType schedule;

    //rucio and clouds
    std::unique_ptr<CRucio> rucio(new CRucio);
    std::unique_ptr<gcp::CCloud> gcp(new gcp::CCloud);

    //proccesses
    std::unique_ptr<CBillingGenerator> billingGen(new CBillingGenerator(gcp.get()));
    std::unique_ptr<CDataGenerator> dataGen(new CDataGenerator(rucio.get(), RNGEngine.get(), 75));

    std::unique_ptr<CTransferManager> g2cTransferMgr(new CTransferManager);
    std::unique_ptr<CTransferManager> c2gTransferMgr(new CTransferManager);

    std::unique_ptr<CTransferNumberGenerator> transferNumberGen(new CTransferNumberGenerator(RNGEngine.get(), 15, 600, transferGenerationDelay, 0.075));
    std::unique_ptr<CTransferNumberGenerator> transferNumberGen2(new CTransferNumberGenerator(RNGEngine.get(), 15, 50, transferGenerationDelay*4, 0.075));
    std::unique_ptr<CTransferGenerator> g2cTransferGen(new CTransferGenerator(RNGEngine.get(), g2cTransferMgr.get(), transferNumberGen.get(), transferGenerationDelay));
    std::unique_ptr<CTransferGenerator> c2gTransferGen(new CTransferGenerator(RNGEngine.get(), c2gTransferMgr.get(), transferNumberGen2.get(), transferGenerationDelay*4));

    std::unique_ptr<CReaper> reaper(new CReaper(rucio.get()));

    std::unique_ptr<CHearbeat> heartbeat(new CHearbeat(rucio.get(), g2cTransferMgr.get(), c2gTransferMgr.get(), 10000));
    heartbeat->mNextCallTick = 10000;
    heartbeat->mProccessDurations["DataGen"] = &(dataGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["G2CTransferUpdate"] = &(g2cTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["C2GTransferUpdate"] = &(c2gTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["G2CTransferGen"] = &(g2cTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["C2GTransferGen"] = &(c2gTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["Reaper"] = &(reaper->mUpdateDurationSummed);

    gcp->SetupDefaultCloud();

    dataGen->mStorageElements.push_back(rucio->CreateGridSite("ASGC", "asia")->CreateStorageElement("TAIWAN_DATADISK"));
    dataGen->mStorageElements.push_back(rucio->CreateGridSite("CERN", "europe")->CreateStorageElement("CERN_DATADISK"));
    dataGen->mStorageElements.push_back(rucio->CreateGridSite("BNL", "us")->CreateStorageElement("BNL_DATADISK"));

    g2cTransferGen->mSrcStorageElements = dataGen->mStorageElements;
    for (auto& region : gcp->mRegions)
        for (auto& bucket : region->mStorageElements)
            g2cTransferGen->mDstStorageElements.push_back(bucket.get());

    c2gTransferGen->mSrcStorageElements = g2cTransferGen->mDstStorageElements;
    c2gTransferGen->mDstStorageElements = g2cTransferGen->mSrcStorageElements;

    for(auto& gridSite : rucio->mGridSites)
    {
        for(auto& region : gcp->mRegions)
        {
            gridSite->CreateLinkSelector(region.get(), ONE_GiB / 256);
            region->CreateLinkSelector(gridSite.get(), ONE_GiB / 256);
        }
    }

    schedule.push(billingGen.get());
    schedule.push(g2cTransferMgr.get());
    schedule.push(c2gTransferMgr.get());
    schedule.push(dataGen.get());
    schedule.push(reaper.get());
    schedule.push(g2cTransferGen.get());
    schedule.push(c2gTransferGen.get());
    schedule.push(heartbeat.get());

    const std::uint64_t MAX_TICK = 3600 * 24 * 31;
    std::uint64_t curTick = 0;
    while(curTick<MAX_TICK && !schedule.empty())
    {
        CScheduleable *element = schedule.top();
        schedule.pop();
        assert(curTick <= element->mNextCallTick);
        curTick = element->mNextCallTick;
        element->OnUpdate(schedule, curTick);
    }
	int a;
	std::cin >> a;
}
