#include <iostream>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cassert>
#include <memory>
#include <random>
#include <unordered_map>

//#define ENABLE_PLOTS
#ifdef ENABLE_PLOTS
#include <mgl2/qt.h>
#endif

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
    std::normal_distribution<double> mFileSizeRNG {1<<24, 4};
    std::normal_distribution<float> mFileLifetimeRNG {3600*24*6, 4};
    const std::uint32_t mTickFreq;

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
            const std::uint32_t fileSize = static_cast<std::uint32_t>( std::abs(mFileSizeRNG(*mRNGEngine)) );
            const std::uint64_t expiresAt = now + static_cast<std::uint64_t>( std::abs(mFileLifetimeRNG(*mRNGEngine)) );

            SFile& file = mRucio->CreateFile(fileSize, expiresAt);
            bytesOfFilesGen += fileSize;

            auto reverseRSEIt = mStorageElements.rbegin();
            //numReplicasPerFile <= numStorageElements !
            for(std::uint32_t numCreated = 0; numCreated<numReplicasPerFile; ++numCreated)
            {
                auto selectedElementIt = mStorageElements.begin() + (rngSampler(*mRNGEngine) % (numStorageElements - numCreated));
                (*selectedElementIt)->CreateReplica(&file)->Increase(fileSize, now);
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
        const std::uint32_t totalFilesToGen = std::max(static_cast<std::uint32_t>(mNumFilesRNG(*mRNGEngine)), 1U);
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
        : mRucio(rucio)
    {
        mNextCallTick = 600;
    }

    void OnUpdate(ScheduleType& schedule, std::uint64_t now) final
    {
        //std::cout<<mRucio->mFiles.size()<<" files stored at "<<now<<std::endl;
        n += mRucio->RunReaper(now);
        //std::cout<<numDeleted<<" reaper deletions at "<<now<<std::endl;
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

        STransfer(CLinkSelector* linkSelector, SReplica* dstReplica)
            : mLinkSelector(linkSelector),
              mDstReplica(dstReplica)
        {}
    };



    std::uint64_t mLastUpdated = 0;
    std::vector<STransfer> mActiveTransfers;

public:
    CTransferManager()
    {
        mActiveTransfers.reserve(1024);
    }

    void CreateTransfer(CLinkSelector* linkSelector, SReplica* dstReplica)
    {
        linkSelector->mNumActiveTransfers += 1;
        mActiveTransfers.emplace_back(linkSelector, dstReplica);
    }
    void CreateTransfer(CStorageElement* srcStorageElement, SReplica* dstReplica)
    {
        CStorageElement* dstStorageElement = dstReplica->GetStorageElement();
        CLinkSelector* linkSelector = srcStorageElement->GetSite()->GetLinkSelector(dstStorageElement->GetSite());
        CreateTransfer(linkSelector, dstReplica);
    }

    void OnUpdate(ScheduleType& schedule, std::uint64_t now) final
    {
		const std::uint32_t timeDiff = static_cast<std::uint32_t>(now - mLastUpdated);
        mLastUpdated = now;
        for(std::size_t idx=0; idx<mActiveTransfers.size(); ++idx)
        {
            CLinkSelector* const linkSelector = mActiveTransfers[idx].mLinkSelector;
            std::uint32_t amount = static_cast<std::uint32_t>((linkSelector->mBandwidth / static_cast<double>(linkSelector->mNumActiveTransfers)) * timeDiff);

            SReplica* const dstReplica = mActiveTransfers[idx].mDstReplica;
            const std::uint32_t maxSize = dstReplica->GetFile()->GetSize();
            const std::uint32_t oldSize = dstReplica->GetCurSize();
            if(dstReplica->Increase(amount, now) == maxSize)
            {
                amount = maxSize - oldSize;
                mActiveTransfers[idx] = std::move(mActiveTransfers.back());
                mActiveTransfers.pop_back();
                linkSelector->mNumActiveTransfers -= 1;
            }
            linkSelector->mUsedTraffic += amount;
        }
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
        assert(mSrcStorageElements.size() > 0);
        const std::uint32_t numActive = static_cast<std::uint32_t>(mTransferMgr->GetNumActiveTransfers());
        const std::uint32_t numToCreate = mTransferNumberGen->GetNumToCreate(now, numActive);
        const std::uint32_t numToCreatePerRSE = 1 + static_cast<std::uint32_t>( numToCreate/static_cast<double>(mSrcStorageElements.size()) );
        std::uint32_t totalTransfersCreated = 0;
        std::uniform_int_distribution<std::size_t> dstStorageElementRndChooser(0, mDstStorageElements.size()-1);
        /*std::unordered_map<std::size_t, std::size_t> indexMap;
        for(auto& srcStorageElement : mSrcStorageElements)
        {
            std::uint32_t numCreated = 0;
            std::size_t numSrcReplicas = srcStorageElement->mReplicas.size();
            std::uniform_int_distribution<std::size_t> rngSampler(0, numSrcReplicas);
            indexMap.clear();
            indexMap.reserve(static_cast<std::size_t>(numSrcReplicas/2.0) + 2);
            while(numSrcReplicas > 0 && numCreated <= numToCreatePerRSE)
            {
                std::size_t idx = rngSampler(*mRNGEngine) % numSrcReplicas;

                --numSrcReplicas;
                auto it = indexMap.find(numSrcReplicas);
                std::size_t endIdx =  (it != indexMap.end()) ? it->second : numSrcReplicas;

                if(idx != numSrcReplicas)
                {
                    auto result = indexMap.insert({idx, endIdx});
                    if(!result.second) //idx already used
                    {
                        idx = result.first->second;
                        result.first->second = endIdx;
                    }
                }
                else // idx addresses "last" element
                {
                    // if numSrcReplicas was used before endIdx will be the resolved idx
                    // if not endIdx will be == numSrcReplicas and wont occur again anyway
                    idx = endIdx;
                }

                SFile* const file = srcStorageElement->mReplicas[idx]->GetFile();
                CStorageElement* const dstStorageElement = mDstStorageElements[dstStorageElementRndChooser(*mRNGEngine)];
                SReplica* const newReplica = dstStorageElement->CreateReplica(file);
                if(newReplica != nullptr)
                {
                    mTransferMgr->CreateTransfer(srcStorageElement, newReplica);
                    ++numCreated;
                }
            }
            totalTransfersCreated += numCreated;
        }*/
        for(auto& srcStorageElement : mSrcStorageElements)
        {
            std::uint32_t numCreated = 0;
            std::size_t numSrcReplicas = srcStorageElement->mReplicas.size();
            std::uniform_int_distribution<std::size_t> rngSampler(0, numSrcReplicas);
            while(numSrcReplicas > 0 && numCreated <= numToCreatePerRSE)
            {
                std::size_t idx = rngSampler(*mRNGEngine) % numSrcReplicas;
                auto& curReplica = srcStorageElement->mReplicas[idx];
                auto& lastReplica = srcStorageElement->mReplicas.back();
                SFile* const file = curReplica->GetFile();
                CStorageElement* const dstStorageElement = mDstStorageElements[dstStorageElementRndChooser(*mRNGEngine)];
                SReplica* const newReplica = dstStorageElement->CreateReplica(file);
                if(newReplica != nullptr)
                {
                    mTransferMgr->CreateTransfer(srcStorageElement, newReplica);
                    ++numCreated;
                }

                --numSrcReplicas;
                std::swap(curReplica->mIndexAtStorageElement, lastReplica->mIndexAtStorageElement);
                std::swap(curReplica, lastReplica);
            }
            totalTransfersCreated += numCreated;
        }
        //std::cout<<"["<<now<<"]\nnumActive: "<<numActive<<"\nnumToCreate: "<<numToCreate<<"\ntotalCreated: "<<totalTransfersCreated<<std::endl;
        mNextCallTick = now + mDelay;
        schedule.push(this);
    }
};

class CHearbeat : public CScheduleable
{
private:
    CRucio* mRucio;
    CTransferManager* mTransferMgr;
    std::uint32_t mDelay;

public:

    CHearbeat(CRucio* rucio, CTransferManager* transferMgr, std::uint32_t delay)
        : mRucio(rucio),
		  mTransferMgr(transferMgr),
          mDelay(delay)
    {}


    void OnUpdate(ScheduleType& schedule, std::uint64_t now) final
    {
        std::cout<<"["<<now<<"]: numFiles="<<mRucio->mFiles.size()<<"; numTransfers="<<mTransferMgr->GetNumActiveTransfers()<<std::endl;

        mNextCallTick = now + mDelay;
        schedule.push(this);
    }
};
#ifdef ENABLE_PLOTS
int sample(mglGraph *gr)
{
    const int n = 1000;
    RNGEngineType RNGEngine(42);
    CTransferGenerator gen(nullptr, RNGEngine);
    mglData y;
    y.Create(n);
    for(int i=0;i<n;++i)
    {
        auto a = gen.GetNumToCreate(i*30, 0);
        y.a[i] = a;
        //std::cout<<a<<std::endl;
    }
    gr->SetFontDef("cursor");
    gr->SetOrigin(0,0,0);
    gr->SetRanges(0,n,0,2000);
    gr->SetTicks('x', 30, 2);
    gr->SetTicks('y', 200, 0);
    //gr->Label('x', "Simulation Time", 1);
    //gr->Label('y', "NumTransfersToCreate", 1);

    //gr->SubPlot(2,2,0,"");
    //gr->Title("Plot plot (default)");

    gr->Axis();
	gr->Box();
    gr->Plot(y);
    return 0;
}
#endif

int main()
{
#ifdef ENABLE_PLOTS
    mglQT gr(sample,"MathGL examples");
    return gr.Run();
#endif
    //std::random_device rngDevice;
    std::unique_ptr<RNGEngineType> RNGEngine(new RNGEngineType(42));

    ScheduleType schedule;

    std::unique_ptr<CRucio> rucio(new CRucio);
    std::unique_ptr<gcp::CCloud> gcp(new gcp::CCloud);

    std::unique_ptr<CBillingGenerator> billingGen(new CBillingGenerator(gcp.get()));
    std::unique_ptr<CReaper> reaper(new CReaper(rucio.get()));
    std::unique_ptr<CTransferManager> transferMgr(new CTransferManager);
    std::unique_ptr<CDataGenerator> dataGen(new CDataGenerator(rucio.get(), RNGEngine.get(), 75));

    const std::uint32_t generationDelay = 50;
    std::unique_ptr<CTransferNumberGenerator> transferNumberGen(new CTransferNumberGenerator(RNGEngine.get(), 15, 600, generationDelay, 0.075));
    std::unique_ptr<CTransferGenerator> g2cTransferGen(new CTransferGenerator(RNGEngine.get(), transferMgr.get(), transferNumberGen.get(), generationDelay));


    gcp->SetupDefaultCloud();

    dataGen->mStorageElements.push_back(rucio->CreateGridSite("ASGC", "asia")->CreateStorageElement("TAIWAN_DATADISK"));
    dataGen->mStorageElements.push_back(rucio->CreateGridSite("CERN", "europe")->CreateStorageElement("CERN_DATADISK"));
    dataGen->mStorageElements.push_back(rucio->CreateGridSite("BNL", "us")->CreateStorageElement("BNL_DATADISK"));

    for(auto& gridSite : rucio->mGridSites)
        for(auto& region : gcp->mRegions)
            gridSite->CreateLinkSelector(region.get(), ONE_GiB);


    g2cTransferGen->mSrcStorageElements = dataGen->mStorageElements;
    for(auto& region : gcp->mRegions)
        for(auto& bucket : region->mStorageElements)
            g2cTransferGen->mDstStorageElements.push_back(bucket.get());

    schedule.push(billingGen.get());
    schedule.push(transferMgr.get());
    schedule.push(dataGen.get());
    schedule.push(reaper.get());
    schedule.push(g2cTransferGen.get());
    std::unique_ptr<CHearbeat> heartbeat(new CHearbeat(rucio.get(), transferMgr.get(), 10000));
    schedule.push(heartbeat.get());

    const std::uint64_t MAX_TICK = 200000;//3600 * 24 * 31;
    std::uint64_t curTick = 0;
    while(curTick<MAX_TICK && !schedule.empty())
    {
        CScheduleable *element = schedule.top();
        schedule.pop();
        assert(curTick <= element->mNextCallTick);
        curTick = element->mNextCallTick;
        element->OnUpdate(schedule, curTick);
    }
}
