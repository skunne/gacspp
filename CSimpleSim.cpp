#include <algorithm>
#include <cassert>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <unordered_map>

#include "constants.h"
#include "CCloudGCP.hpp"
#include "CRucio.hpp"
#include "CScheduleable.hpp"
#include "CSimpleSim.hpp"


class CDataGenerator : public CScheduleable
{
private:
    IBaseSim* mSim;

    std::normal_distribution<float> mNumFilesRNG {20, 1};
    std::normal_distribution<double> mFileSizeRNG {1, 0.5};
    std::normal_distribution<float> mFileLifetimeRNG {6, 1};

    std::uint32_t mTickFreq;

    inline std::uint32_t GetRandomFileSize()
    {
        const double min = ONE_GiB / 16;
        const double max = static_cast<double>(std::numeric_limits<std::uint32_t>::max());
        const double val = GiB_TO_BYTES(std::abs(mFileSizeRNG(mSim->mRNGEngine)));
        return static_cast<std::uint32_t>(std::clamp(val, min, max));
    }
    inline std::uint32_t GetRandomNumFilesToGenerate()
    {
        return static_cast<std::uint32_t>( std::max(1.f, mNumFilesRNG(mSim->mRNGEngine)) );
    }
    inline std::uint64_t GetRandomLifeTime()
    {
        float val = DAYS_TO_SECONDS(std::abs(mFileLifetimeRNG(mSim->mRNGEngine)));
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

            SFile* const file = mSim->mRucio->CreateFile(fileSize, expiresAt);
            bytesOfFilesGen += fileSize;

            auto reverseRSEIt = mStorageElements.rbegin();
            //numReplicasPerFile <= numStorageElements !
            for(std::uint32_t numCreated = 0; numCreated<numReplicasPerFile; ++numCreated)
            {
                auto selectedElementIt = mStorageElements.begin() + (rngSampler(mSim->mRNGEngine) % (numStorageElements - numCreated));
                (*selectedElementIt)->CreateReplica(file)->Increase(fileSize, now);
                std::iter_swap(selectedElementIt, reverseRSEIt);
				++reverseRSEIt;
            }
        }
        return bytesOfFilesGen;
    }

public:
    std::vector<CStorageElement*> mStorageElements;
    CDataGenerator(IBaseSim* sim, const std::uint32_t tickFreq, const CScheduleable::TickType startTick=0)
        : CScheduleable(startTick),
          mSim(sim),
          mTickFreq(tickFreq)
    {}

    void OnUpdate(const CScheduleable::TickType now) final
    {
        auto curRealtime = std::chrono::high_resolution_clock::now();
        const std::uint32_t totalFilesToGen = GetRandomNumFilesToGenerate();

        std::uint32_t numFilesToGen = std::max(static_cast<std::uint32_t>(totalFilesToGen * 0.6f), 1U);
        CreateFilesAndReplicas(numFilesToGen, 1, now);

        numFilesToGen = totalFilesToGen - numFilesToGen;
        CreateFilesAndReplicas(numFilesToGen, 2, now);

        mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
        mNextCallTick = now + mTickFreq;
    }
};

class CReaper : public CScheduleable
{
private:
    CRucio *mRucio;

    std::uint32_t mTickFreq;

public:
    CReaper(CRucio *rucio, const std::uint32_t tickFreq, const CScheduleable::TickType startTick=600)
        : CScheduleable(startTick),
          mRucio(rucio),
          mTickFreq(tickFreq)
    {}

    void OnUpdate(const CScheduleable::TickType now) final
    {
        auto curRealtime = std::chrono::high_resolution_clock::now();
        mRucio->RunReaper(now);

        mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
        mNextCallTick = now + mTickFreq;
    }
};

class CBillingGenerator : public CScheduleable
{
private:
    IBaseCloud* mCloud;

    std::uint32_t mTickFreq;

public:
    CBillingGenerator(IBaseCloud* cloud, const std::uint32_t tickFreq=SECONDS_PER_MONTH, const CScheduleable::TickType startTick=SECONDS_PER_MONTH)
        : CScheduleable(startTick),
          mCloud(cloud),
          mTickFreq(tickFreq)
    {}

    void OnUpdate(const CScheduleable::TickType now) final
    {
        std::cout<<mCloud->GetName()<<" - Billing for Month "<<(now/SECONDS_PER_MONTH)<<":\n";
        auto res = mCloud->ProcessBilling(now);
        std::cout<<"\tStorage: "<<res.first<<std::endl;
        std::cout<<"\tNetwork: "<<res.second<<std::endl;

        mNextCallTick = now + mTickFreq;
    }
};

class CTransferManager : public CScheduleable
{
private:
    CScheduleable::TickType mLastUpdated = 0;
    std::uint32_t mTickFreq;

    struct STransfer
    {
        CLinkSelector* mLinkSelector;
        SReplica* mDstReplica;
        CScheduleable::TickType mStartTick;

        STransfer(CLinkSelector* linkSelector, SReplica* dstReplica, CScheduleable::TickType startTick)
            : mLinkSelector(linkSelector),
              mDstReplica(dstReplica),
              mStartTick(startTick)
        {}
    };

    void DeleteTransferUnordered(const std::size_t idx)
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
    std::vector<STransfer> mActiveTransfers;

public:
    std::uint32_t mNumCompletedTransfers = 0;
    std::uint64_t mSummedTransferDuration = 0;
    CTransferManager(const std::uint32_t tickFreq, const CScheduleable::TickType startTick=0)
        : CScheduleable(startTick),
          mTickFreq(tickFreq)
    {
        mActiveTransfers.reserve(1024);
    }

    void CreateTransfer(CLinkSelector* const linkSelector, SReplica* const dstReplica, const CScheduleable::TickType now)
    {
        assert(mActiveTransfers.size() < mActiveTransfers.capacity());
        linkSelector->mNumActiveTransfers += 1;
        mActiveTransfers.emplace_back(linkSelector, dstReplica, now);
        dstReplica->mTransferRef = &(mActiveTransfers.back().mDstReplica);
    }
    void CreateTransfer(CStorageElement* const srcStorageElement, SReplica* const dstReplica, const CScheduleable::TickType now)
    {
        CStorageElement* dstStorageElement = dstReplica->GetStorageElement();
        CLinkSelector* linkSelector = srcStorageElement->GetSite()->GetLinkSelector(dstStorageElement->GetSite());
        CreateTransfer(linkSelector, dstReplica, now);
    }

    void OnUpdate(const CScheduleable::TickType now) final
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
        mNextCallTick = now + mTickFreq;
    }
    inline auto GetNumActiveTransfers() const -> std::size_t
    {return mActiveTransfers.size();}
};

class CTransferNumberGenerator
{
public:
    double mSoftmaxScale;
    double mSoftmaxOffset;
    double mAlpha = 1.0/30.0 * PI/180.0 * 0.075;

    std::normal_distribution<double> mSoftmaxRNG {0, 1};
    std::normal_distribution<double> mPeakinessRNG {1.05, 0.04};

    CTransferNumberGenerator(const double softmaxScale, const double softmaxOffset, const std::uint32_t samplingFreq, const double baseFreq)
    : mSoftmaxScale(softmaxScale),
      mSoftmaxOffset(softmaxOffset),
      mAlpha(1.0/samplingFreq * PI/180.0 * baseFreq)
    {}

    inline std::uint32_t GetNumToCreate(IBaseSim::RNGEngineType& rngEngine, std::uint32_t numActive, const CScheduleable::TickType now)
    {
        const double softmax = (std::cos(now * mAlpha) * mSoftmaxScale + mSoftmaxOffset) * (1.0 + mSoftmaxRNG(rngEngine) * 0.02);
        const double diffSoftmaxActive = softmax - numActive;
        if(diffSoftmaxActive < 0.5)
            return 0;
        return static_cast<std::uint32_t>(std::pow(diffSoftmaxActive, abs(mPeakinessRNG(rngEngine))));
    }
};

class CTransferGenerator : public CScheduleable
{
private:
    IBaseSim* mSim;
    CTransferManager* mTransferMgr;
    std::uint32_t mTickFreq;

public:
    std::unique_ptr<CTransferNumberGenerator> mTransferNumberGen;
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<CStorageElement*> mDstStorageElements;

    CTransferGenerator(IBaseSim* sim, CTransferManager* transferMgr, const std::uint32_t tickFreq, const CScheduleable::TickType startTick=0)
        : CScheduleable(startTick),
          mSim(sim),
          mTransferMgr(transferMgr),
          mTickFreq(tickFreq),
          mTransferNumberGen(new CTransferNumberGenerator(1, 1, tickFreq, 0.075))
    {}

    void OnUpdate(const CScheduleable::TickType now) final
    {
        auto curRealtime = std::chrono::high_resolution_clock::now();
        assert(mSrcStorageElements.size() > 0);
        const std::uint32_t numActive = static_cast<std::uint32_t>(mTransferMgr->GetNumActiveTransfers());
        const std::uint32_t numToCreate = mTransferNumberGen->GetNumToCreate(mSim->mRNGEngine, numActive, now);
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
                const std::size_t idx = rngSampler(mSim->mRNGEngine) % numSrcReplicas;
                --numSrcReplicas;

                auto& curReplica = srcStorageElement->mReplicas[idx];
                if(curReplica->IsComplete())
                {
                    CStorageElement* const dstStorageElement = mDstStorageElements[dstStorageElementRndChooser(mSim->mRNGEngine)];
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
        mNextCallTick = now + mTickFreq;
    }
};

class CHearbeat : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mG2CTransferMgr;
    std::uint32_t mTickFreq;

    std::chrono::high_resolution_clock::time_point mTimeLastUpdate;

public:
    std::unordered_map<std::string, std::chrono::duration<double>*> mProccessDurations;

    CHearbeat(IBaseSim* sim, std::shared_ptr<CTransferManager> g2cTransferMgr, const std::uint32_t tickFreq, const CScheduleable::TickType startTick=0)
        : CScheduleable(startTick),
          mSim(sim),
          mG2CTransferMgr(g2cTransferMgr),
          mTickFreq(tickFreq)
    {
        mTimeLastUpdate = std::chrono::high_resolution_clock::now();
    }


    void OnUpdate(const CScheduleable::TickType now) final
    {
        auto curRealtime = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> timeDiff = curRealtime - mTimeLastUpdate;
        mUpdateDurationSummed += timeDiff;
        mTimeLastUpdate = curRealtime;

        std::stringstream statusOutput;
        statusOutput << std::fixed << std::setprecision(2);

        std::shared_ptr<CTransferManager> mC2GTransferMgr = mG2CTransferMgr;
        statusOutput << "[" << std::setw(6) << static_cast<std::uint32_t>(now / 1000) << "k]: ";
        statusOutput << "Runtime: " << mUpdateDurationSummed.count() << "s; ";
        statusOutput << "numFiles: " << mSim->mRucio->mFiles.size() / 1000.0 << "k; ";
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

        mNextCallTick = now + mTickFreq;
    }
};


void CSimpleSim::SetupDefaults()
{
    //proccesses
    std::vector<std::shared_ptr<CBillingGenerator>> billingGenerators;
    std::shared_ptr<CDataGenerator> dataGen(new CDataGenerator(this, 75, 0));
    std::shared_ptr<CReaper> reaper(new CReaper(mRucio.get(), 600, 600));


    std::shared_ptr<CTransferManager> g2cTransferMgr(new CTransferManager(20, 100));
    std::shared_ptr<CTransferGenerator> g2cTransferGen(new CTransferGenerator(this, g2cTransferMgr.get(), 50));
    g2cTransferGen->mTransferNumberGen->mSoftmaxScale = 15;
    g2cTransferGen->mTransferNumberGen->mSoftmaxOffset = 600;

    //std::shared_ptr<CTransferManager> c2gTransferMgr(new CTransferManager);
    //std::shared_ptr<CTransferGenerator> c2gTransferGen(new CTransferGenerator(this c2gTransferMgr.get(), transferGenerationFreq*4));
    //c2gTransferGen->mTransferNumberGen->mSoftmaxScale = 10;
    //c2gTransferGen->mTransferNumberGen->mSoftmaxOffset = 50;

    std::shared_ptr<CHearbeat> heartbeat(new CHearbeat(this, g2cTransferMgr, 10000, 10000));
    heartbeat->mProccessDurations["DataGen"] = &(dataGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["G2CTransferUpdate"] = &(g2cTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["G2CTransferGen"] = &(g2cTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["Reaper"] = &(reaper->mUpdateDurationSummed);


    mRucio.reset(new CRucio);

    CGridSite* asgc = mRucio->CreateGridSite("ASGC", "asia");
    CGridSite* cern = mRucio->CreateGridSite("CERN", "europe");
    CGridSite* bnl = mRucio->CreateGridSite("BNL", "us");

    std::vector<CStorageElement*> gridStoragleElements {
                asgc->CreateStorageElement("TAIWAN_DATADISK"),
                cern->CreateStorageElement("CERN_DATADISK"),
                bnl->CreateStorageElement("BNL_DATADISK")
            };

    for(auto& cloud : mClouds)
    {
        cloud->SetupDefaultCloud();
        for(auto& gridSite : mRucio->mGridSites)
        {
            for(auto& cloudSite : cloud->mRegions)
            {
                auto region = dynamic_cast<gcp::CRegion*>(cloudSite.get());
                gridSite->CreateLinkSelector(region, ONE_GiB / 256);
                region->CreateLinkSelector(gridSite.get(), ONE_GiB / 256);
                for (auto& bucket : region->mStorageElements)
                    g2cTransferGen->mDstStorageElements.push_back(bucket.get());
            }
        }
        mSchedule.emplace(new CBillingGenerator(cloud.get()));
    }
    g2cTransferGen->mSrcStorageElements = gridStoragleElements;
    dataGen->mStorageElements = gridStoragleElements;

    //c2gTransferGen->mSrcStorageElements = g2cTransferGen->mDstStorageElements;
    //c2gTransferGen->mDstStorageElements = g2cTransferGen->mSrcStorageElements;

    mSchedule.push(dataGen);
    mSchedule.push(reaper);
    mSchedule.push(g2cTransferMgr);
    mSchedule.push(g2cTransferGen);
    mSchedule.push(heartbeat);
}
