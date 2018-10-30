#include <iostream>
#include <algorithm>
#include <cstdint>
#include <cassert>
#include <random>
#include <memory>

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
    CRucio *mRucio;
    RNGEngineType* mRNGEngine;
    std::normal_distribution<float> mNumFilesRNG {25, 1};
    std::normal_distribution<double> mFileSizeRNG {1<<24, 4};
    std::normal_distribution<float> mFileLifetimeRNG {3600*24*5, 1};
    const std::uint32_t mTickFreq;

    std::uint64_t CreateFilesAndReplicas(std::uint32_t numFiles, std::uint32_t numReplicasPerFile, std::uint64_t now)
    {
        if(numFiles == 0 || numReplicasPerFile == 0)
            return 0;
        const std::uint32_t numStorageElements = static_cast<std::uint32_t>(mStorageElements.size());
        assert(numReplicasPerFile <= numStorageElements);
        std::uint64_t bytesOfFilesGen = 0;
        for(std::uint32_t i = 0; i < numFiles; ++i)
        {
            const std::uint32_t fileSize = static_cast<std::uint32_t>(mFileSizeRNG(*mRNGEngine));
            const std::uint64_t expiresAt = now + static_cast<std::uint64_t>(mFileLifetimeRNG(*mRNGEngine));
            SFile& fileObj = mRucio->CreateFile(fileSize, expiresAt);
            bytesOfFilesGen += fileSize;
            auto reverseRSEIt = mStorageElements.rbegin();
            std::uint32_t idxOffset = 1;
            while(idxOffset <= numReplicasPerFile && reverseRSEIt != mStorageElements.rend())
            {
                std::uniform_int_distribution<std::uint32_t> rngSampler(0, numStorageElements - idxOffset);
                auto selectedElementIt = mStorageElements.begin() + rngSampler(*mRNGEngine);
                (*selectedElementIt)->CreateReplica(fileObj).Increase(fileSize, now);
				//fileObj.CreateReplica(**selectedElementIt);
                std::iter_swap(selectedElementIt, reverseRSEIt);
				++idxOffset;
				++reverseRSEIt;
            }
        }
        return bytesOfFilesGen;
    }

public:
    std::vector<CStorageElement*> mStorageElements;
    CDataGenerator(CRucio *rucio, RNGEngineType* RNGEngine, const std::uint32_t tickFreq)
    :   mRucio(rucio),
        mRNGEngine(RNGEngine),
        mTickFreq(tickFreq)
    {}

    void OnUpdate(ScheduleType& schedule, std::uint64_t now) final
    {
        const std::uint32_t totalFilesToGen = std::max(static_cast<std::uint32_t>(mNumFilesRNG(*mRNGEngine)), 1U);

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
    gcp::CCloud *mCloud;

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
    CTransferManager* mTransferMgr;
    CTransferNumberGenerator* mTransferNumberGen;
    std::uint32_t mDelay;

public:
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<CStorageElement*> mDstStorageElements;

    CTransferGenerator(CTransferManager* transferMgr, CTransferNumberGenerator* transferNumberGen, std::uint32_t delay)
        : mTransferMgr(transferMgr),
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
        for(const auto& storageElement : mSrcStorageElements)
        {
        }
        /*
        # generate grid -> cloud

        for grid_rse_obj in self.grid_rses:
            num_files = min(len(grid_rse_obj.replica_list), num_to_create_per_rse)
            if (num_files + total_transfers_created) > num_to_create:
                num_files = num_to_create - total_transfers_created
            if num_files <= 0:
                continue
            total_transfers_created += num_files
            replicas = random.sample(grid_rse_obj.replica_list, num_files)
            cloud_rse_obj = npr.choice(self.cloud.bucket_list)
            for replica in replicas:
                if cloud_rse_obj.name in replica.file.rse_by_name:
                    #log.warning('{} to {}'.format(grid_rse_obj.name, cloud_rse_obj.name))
                    continue
                self.new_transfers.append(self.rucio.create_transfer(replica.file, grid_rse_obj, cloud_rse_obj))
        #log.debug('active: {}, to_create: {}, created: 0'.format(num_active, num_to_create), self.sim.now)

        # generate cloud -> cloud
            # 1. same multi regional location
            # 2. between multi regional locations
        # generate cloud -> else
        */
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
    std::unique_ptr<CDataGenerator> dataGen(new CDataGenerator(rucio.get(), RNGEngine.get(), 50));

    const std::uint32_t generationDelay = 30;
    std::unique_ptr<CTransferNumberGenerator> transferNumberGen(new CTransferNumberGenerator(RNGEngine.get(), 15, 600, generationDelay, 0.075));
    std::unique_ptr<CTransferGenerator> g2cTransferGen(new CTransferGenerator(transferMgr.get(), transferNumberGen.get(), generationDelay));


    gcp->SetupDefaultCloud();

    dataGen->mStorageElements.push_back(&(rucio->CreateGridSite("ASGC", "asia").CreateStorageElement("TAIWAN_DATADISK")));
    dataGen->mStorageElements.push_back(&(rucio->CreateGridSite("CERN", "europe").CreateStorageElement("CERN_DATADISK")));
    dataGen->mStorageElements.push_back(&(rucio->CreateGridSite("BNL", "us").CreateStorageElement("BNL_DATADISK")));

    g2cTransferGen->mSrcStorageElements = dataGen->mStorageElements;
    for(auto& region : gcp->mRegions)
        for(auto& bucket : region.mStorageElements)
            g2cTransferGen->mDstStorageElements.push_back(&bucket);

    schedule.push(billingGen.get());
    schedule.push(transferMgr.get());
    schedule.push(dataGen.get());
    schedule.push(reaper.get());
    schedule.push(g2cTransferGen.get());

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
	std::cout << reaper->n << std::endl;
	std::cin >> reaper->n;
}
