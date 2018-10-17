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

/*
class TransferNumGenerator:
    def __init__(self):
        self.DELAY_BASE = 30
        self.ALPHA = 1/self.DELAY_BASE * np.pi/180 * 0.075
        self.SCALE_OF_SOFTMAX = 15
        self.OFFSET_OF_SOFTMAX = 600
        self.GEN_BUNCH_SIZE = 10000
        self.idx_offset = 0
        self.softmax_values = np.empty(self.GEN_BUNCH_SIZE)
        self.make_values(0)

    def make_values(self, start_val):
        step_size = float(self.DELAY_BASE)
        end_val = start_val + self.GEN_BUNCH_SIZE * step_size
        vals = np.arange(start_val, end_val, step_size)
        vals *= self.ALPHA
        np.cos(vals, out=self.softmax_values)
        self.softmax_values *= self.SCALE_OF_SOFTMAX
        self.softmax_values += self.OFFSET_OF_SOFTMAX
        self.softmax_values += npr.normal(0, 1, len(vals)) * self.softmax_values * 0.02

    def get_num_to_create(self, cur_time, num_active):
        idx = int(cur_time / self.DELAY_BASE)
        idx -= self.idx_offset
        if idx >= len(self.softmax_values):
            self.make_values(cur_time)
            self.idx_offset += self.GEN_BUNCH_SIZE
            idx -= self.GEN_BUNCH_SIZE
        diff_softmax_active = self.softmax_values[idx] - num_active
        if diff_softmax_active <= 0:
            return 0
        return int(diff_softmax_active ** abs(npr.normal(1.05, 0.04)))
*/

class CDataGenerator : public CScheduleable
{
private:
    CRucio *mRucio;
    RNGEngineType &mRNGEngine;
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
            const std::uint32_t fileSize = static_cast<std::uint32_t>(mFileSizeRNG(mRNGEngine));
            const std::uint64_t expiresAt = now + static_cast<std::uint64_t>(mFileLifetimeRNG(mRNGEngine));
            SFile& fileObj = mRucio->CreateFile(fileSize, expiresAt);
            bytesOfFilesGen += fileSize;
            auto reverseRSEIt = mStorageElements.rbegin();
            std::uint32_t idxOffset = 1;
            while(idxOffset <= numReplicasPerFile && reverseRSEIt != mStorageElements.rend())
            {
                std::uniform_int_distribution<std::uint32_t> rngSampler(0, numStorageElements - idxOffset);
				std::uint32_t a = rngSampler(mRNGEngine);
                auto selectedElementIt = mStorageElements.begin() + a;
                //(*selectedElementIt)->CreateReplica(fileObj).Increase(fileSize, now);
				fileObj.CreateReplica(**selectedElementIt);
                std::iter_swap(selectedElementIt, reverseRSEIt);
				++idxOffset;
				++reverseRSEIt;
            }
        }
        return bytesOfFilesGen;
    }

public:
    std::vector<CStorageElement*> mStorageElements;
    CDataGenerator(CRucio *rucio, RNGEngineType &RNGEngine, const std::uint32_t tickFreq)
    :   mRucio(rucio),
        mRNGEngine(RNGEngine),
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
        : mRucio(rucio)
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

    virtual void OnUpdate(ScheduleType &schedule, std::uint64_t now)
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

    virtual void OnUpdate(ScheduleType &schedule, std::uint64_t now)
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
                if(mActiveTransfers.size() > 1)
                    mActiveTransfers[idx] = std::move(mActiveTransfers.back());
                mActiveTransfers.pop_back();
            }
            linkSelector->mUsedTraffic += amount;
        }
        mNextCallTick = now + 30;
        schedule.push(this);
    }
};

class CTransferGenerator : public CScheduleable
{
private:
    CTransferManager* mTransferMgr;

public:
    CTransferGenerator(CTransferManager* transferMgr)
        : mTransferMgr(transferMgr)
    {}

    virtual void OnUpdate(ScheduleType &schedule, std::uint64_t now)
    {
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
    std::unique_ptr<gcp::CCloud> gcp(new gcp::CCloud);

    std::unique_ptr<CBillingGenerator> billingGen(new CBillingGenerator(gcp.get()));
    std::unique_ptr<CReaper> reaper(new CReaper(rucio.get()));
    std::unique_ptr<CTransferManager> transferMgr(new CTransferManager);
    std::unique_ptr<CDataGenerator> dataGen(new CDataGenerator(rucio.get(), RNGEngine, 50));

    gcp->SetupDefaultCloud();

    dataGen->mStorageElements.push_back(&(rucio->CreateGridSite("ASGC", "asia").CreateStorageElement("TAIWAN_DATADISK")));
    dataGen->mStorageElements.push_back(&(rucio->CreateGridSite("CERN", "europe").CreateStorageElement("CERN_DATADISK")));
    dataGen->mStorageElements.push_back(&(rucio->CreateGridSite("BNL", "us").CreateStorageElement("BNL_DATADISK")));


    schedule.push(billingGen.get());
    schedule.push(transferMgr.get());
    schedule.push(dataGen.get());
    schedule.push(reaper.get());

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
