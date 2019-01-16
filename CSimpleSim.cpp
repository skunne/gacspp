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
#include "COutput.hpp"
#include "CScheduleable.hpp"
#include "CSimpleSim.hpp"

#include "sqlite3.h"



class CDataGenerator : public CScheduleable
{
private:
    std::size_t mOutputQueryIdx;

    IBaseSim* mSim;

    std::normal_distribution<float> mNumFilesRNG {40, 1};
    std::normal_distribution<double> mFileSizeRNG {1.5, 0.25};
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
    inline TickType GetRandomLifeTime()
    {
        float val = DAYS_TO_SECONDS(std::abs(mFileLifetimeRNG(mSim->mRNGEngine)));
        return static_cast<TickType>( std::max(float(SECONDS_PER_DAY), val) );
    }

    std::uint64_t CreateFilesAndReplicas(const std::uint32_t numFiles, const std::uint32_t numReplicasPerFile, const TickType now)
    {
        if(numFiles == 0 || numReplicasPerFile == 0)
            return 0;

        const std::uint32_t numStorageElements = static_cast<std::uint32_t>( mStorageElements.size() );

        assert(numReplicasPerFile <= numStorageElements);

        CInsertStatements* outputs = new CInsertStatements(mOutputQueryIdx, numFiles * 4);
        std::uniform_int_distribution<std::uint32_t> rngSampler(0, numStorageElements);
        std::uint64_t bytesOfFilesGen = 0;
        for(std::uint32_t i = 0; i < numFiles; ++i)
        {
            const std::uint32_t fileSize = GetRandomFileSize();
            const TickType lifetime = GetRandomLifeTime();

            SFile* const file = mSim->mRucio->CreateFile(fileSize, now + lifetime);

            outputs->AddValue(file->GetId());
            outputs->AddValue(now);
            outputs->AddValue(lifetime);
            outputs->AddValue(fileSize);

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
        COutput::GetRef().QueueInserts(outputs);
        return bytesOfFilesGen;
    }

public:
    std::vector<CStorageElement*> mStorageElements;
    CDataGenerator(IBaseSim* sim, const std::uint32_t tickFreq, const TickType startTick=0)
        : CScheduleable(startTick),
          mSim(sim),
          mTickFreq(tickFreq)
    {
        mOutputQueryIdx = COutput::GetRef().AddPreparedSQLStatement("INSERT INTO Files VALUES(?, ?, ?, ?);");
    }

    void OnUpdate(const TickType now) final
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
    CReaper(CRucio *rucio, const std::uint32_t tickFreq, const TickType startTick=600)
        : CScheduleable(startTick),
          mRucio(rucio),
          mTickFreq(tickFreq)
    {}

    void OnUpdate(const TickType now) final
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
    IBaseSim* mSim;

    std::uint32_t mTickFreq;

public:
    CBillingGenerator(IBaseSim* sim, const std::uint32_t tickFreq=SECONDS_PER_MONTH, const TickType startTick=SECONDS_PER_MONTH)
        : CScheduleable(startTick),
          mSim(sim),
          mTickFreq(tickFreq)
    {}

    void OnUpdate(const TickType now) final
    {
        const std::string caption = std::string(10, '=') + " Monthly Summary " + std::string(10, '=');
        std::cout << std::endl;
        std::cout << std::string(caption.length(), '=') << std::endl;
        std::cout << caption << std::endl;
        std::cout << std::string(caption.length(), '=') << std::endl;

        std::cout << "-Grid2Cloud Link Stats-" << std::endl;
        for(auto& srcGridSite : mSim->mRucio->mGridSites)
        {
            std::cout << srcGridSite->GetName() << std::endl;
            for (auto& dstRegion : mSim->mClouds[0]->mRegions)
            {
                auto link = srcGridSite->GetLinkSelector(dstRegion.get());
                if (link == nullptr)
                    continue;
                std::cout << "\t--> " << dstRegion->GetName() << ": " << link->mDoneTransfers << std::endl;
                link->mDoneTransfers = 0;
            }
        }
        for(auto& cloud : mSim->mClouds)
        {
            std::cout << std::endl;
            std::cout<<cloud->GetName()<<" - Billing for Month "<<(now/SECONDS_PER_MONTH)<<":\n";
            auto res = cloud->ProcessBilling(now);
            std::cout << "\tStorage: " << res.first << " CHF" << std::endl;
            std::cout << "\tNetwork: " << res.second.first << " CHF" << std::endl;
            std::cout << "\tNetwork: " << res.second.second << " GiB" << std::endl;
        }

        std::cout << std::string(caption.length(), '=') << std::endl;
        std::cout << std::endl;

        mNextCallTick = now + mTickFreq;
    }
};

class CTransferManager : public CScheduleable
{
private:
    std::size_t mOutputQueryIdx;

    TickType mLastUpdated = 0;
    std::uint32_t mTickFreq;

    struct STransfer
    {
        SReplica* mSrcReplica;
        SReplica* mDstReplica;
        CLinkSelector* mLinkSelector;
        TickType mStartTick;

        STransfer(SReplica* const srcReplica, SReplica* const dstReplica, CLinkSelector* const linkSelector, TickType startTick)
            : mSrcReplica(srcReplica),
              mDstReplica(dstReplica),
              mLinkSelector(linkSelector),
              mStartTick(startTick)
        {}
    };

    void DeleteTransferUnordered(const std::size_t idx)
    {
        auto& transfer = mActiveTransfers[idx];
        do {
            transfer.mLinkSelector->mNumActiveTransfers -= 1;
            transfer = std::move(mActiveTransfers.back());
            mActiveTransfers.pop_back();
        } while (transfer.mDstReplica == nullptr && !mActiveTransfers.empty());

        if (!mActiveTransfers.empty())
            transfer.mDstReplica->mTransferRef = &(transfer.mDstReplica);
    }
    std::vector<STransfer> mActiveTransfers;

public:
    //std::ofstream mTransferLog;
    //std::ofstream mTrafficLog;
    std::uint32_t mNumCompletedTransfers = 0;
    TickType mSummedTransferDuration = 0;
    CTransferManager(const std::string& transferLogFilePath, const std::string& trafficLogFilePath, const std::uint32_t tickFreq, const TickType startTick=0)
        : CScheduleable(startTick),
          mTickFreq(tickFreq)//,
          //mTransferLog(transferLogFilePath),
          //mTrafficLog(trafficLogFilePath)
    {
        mActiveTransfers.reserve(2048);
        mOutputQueryIdx = COutput::GetRef().AddPreparedSQLStatement("INSERT INTO Transfers VALUES(?, ?, ?, ?, ?, ?);");
    }

    void CreateTransfer(SReplica* const srcReplica, SReplica* const dstReplica, const TickType now)
    {
        assert(mActiveTransfers.size() < mActiveTransfers.capacity());

        ISite* const srcSite = srcReplica->GetStorageElement()->GetSite();
        ISite* const dstSite = dstReplica->GetStorageElement()->GetSite();
        CLinkSelector* const linkSelector = srcSite->GetLinkSelector(dstSite);

        linkSelector->mNumActiveTransfers += 1;
        mActiveTransfers.emplace_back(srcReplica, dstReplica, linkSelector, now);
        dstReplica->mTransferRef = &(mActiveTransfers.back().mDstReplica);
    }

    void OnUpdate(const TickType now) final
    {
        auto curRealtime = std::chrono::high_resolution_clock::now();
        //mTransferLog << 0 << "|" << now << "|" << mActiveTransfers.size() << "|";
		const std::uint32_t timeDiff = static_cast<std::uint32_t>(now - mLastUpdated);
        mLastUpdated = now;

        std::size_t idx = 0;
        std::uint64_t summedTraffic = 0;
        CInsertStatements* outputs = new CInsertStatements(mOutputQueryIdx, 6 + mActiveTransfers.size());

        while (idx < mActiveTransfers.size())
        {
            STransfer& transfer = mActiveTransfers[idx];
            SReplica* const dstReplica = transfer.mDstReplica;
            CLinkSelector* const linkSelector = transfer.mLinkSelector;
            if (dstReplica == nullptr)
            {
                ++linkSelector->mFailedTransfers;
                DeleteTransferUnordered(idx);
                continue; // handle same idx again
            }

            const double sharedBandwidth = linkSelector->mBandwidth / static_cast<double>(linkSelector->mNumActiveTransfers);
            std::uint32_t amount = static_cast<std::uint32_t>(sharedBandwidth * timeDiff);
            amount = dstReplica->Increase(amount, now);
            summedTraffic += amount;
            linkSelector->mUsedTraffic += amount;

            if(dstReplica->IsComplete())
            {
                outputs->AddValue(GetNewId());
                outputs->AddValue(dstReplica->GetFile()->GetId());
                outputs->AddValue(transfer.mSrcReplica->GetStorageElement()->GetId());
                outputs->AddValue(dstReplica->GetStorageElement()->GetId());
                outputs->AddValue(transfer.mStartTick);
                outputs->AddValue(now);

                ++linkSelector->mDoneTransfers;
                ++mNumCompletedTransfers;
                mSummedTransferDuration += now - transfer.mStartTick;
                transfer.mDstReplica->mTransferRef = nullptr;
                DeleteTransferUnordered(idx);
                continue; // handle same idx again
            }
            ++idx;
        }

        COutput::GetRef().QueueInserts(outputs);
        //mTrafficLog << now << "|" << summedTraffic << "|";

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

    inline std::uint32_t GetNumToCreate(IBaseSim::RNGEngineType& rngEngine, std::uint32_t numActive, const TickType now)
    {
        const double softmax = (std::cos(now * mAlpha) * mSoftmaxScale + mSoftmaxOffset) * (1.0 + mSoftmaxRNG(rngEngine) * 0.02);
        const double diffSoftmaxActive = softmax - numActive;
        if(diffSoftmaxActive < 0.5)
            return 0;
        return static_cast<std::uint32_t>(std::pow(diffSoftmaxActive, abs(mPeakinessRNG(rngEngine))));
    }
};

class CTransferGeneratorUniform : public CScheduleable
{
private:
    IBaseSim* mSim;
    CTransferManager* mTransferMgr;
    std::uint32_t mTickFreq;

public:
    //std::ofstream* mTransferLog;
    std::unique_ptr<CTransferNumberGenerator> mTransferNumberGen;
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<CStorageElement*> mDstStorageElements;

    CTransferGeneratorUniform(IBaseSim* sim, CTransferManager* transferMgr, const std::uint32_t tickFreq, const TickType startTick=0)
        : CScheduleable(startTick),
          mSim(sim),
          mTransferMgr(transferMgr),
          mTickFreq(tickFreq),
          mTransferNumberGen(new CTransferNumberGenerator(1, 1, tickFreq, 0.075))
    {}

    void OnUpdate(const TickType now) final
    {
        auto curRealtime = std::chrono::high_resolution_clock::now();
        assert(mSrcStorageElements.size() > 0);
        IBaseSim::RNGEngineType& rngEngine = mSim->mRNGEngine;
        const std::uint32_t numActive = static_cast<std::uint32_t>(mTransferMgr->GetNumActiveTransfers());
        const std::uint32_t numToCreate = mTransferNumberGen->GetNumToCreate(rngEngine, numActive, now);
        const std::uint32_t numToCreatePerRSE = static_cast<std::uint32_t>( numToCreate/static_cast<double>(mSrcStorageElements.size()) );
        std::uint32_t totalTransfersCreated = 0;
        std::uniform_int_distribution<std::size_t> dstStorageElementRndChooser(0, mDstStorageElements.size()-1);
        for(CStorageElement* srcStorageElement : mSrcStorageElements)
        {
            std::uint32_t numCreated = 0;
            std::size_t numSrcReplicas = srcStorageElement->mReplicas.size();
            std::uniform_int_distribution<std::size_t> rngSampler(0, numSrcReplicas);
            while(numSrcReplicas > 0 && numCreated < numToCreatePerRSE)
            {
                const std::size_t idx = rngSampler(rngEngine) % numSrcReplicas;
                --numSrcReplicas;

                SReplica*& curReplica = srcStorageElement->mReplicas[idx];
                if(curReplica->IsComplete())
                {
                    CStorageElement* const dstStorageElement = mDstStorageElements[dstStorageElementRndChooser(rngEngine)];
                    SReplica* const newReplica = dstStorageElement->CreateReplica(curReplica->GetFile());
                    if(newReplica != nullptr)
                    {
                        mTransferMgr->CreateTransfer(curReplica, newReplica, now);
                        ++numCreated;
                    }
                }
                SReplica*& lastReplica = srcStorageElement->mReplicas[numSrcReplicas];
                std::swap(curReplica->mIndexAtStorageElement, lastReplica->mIndexAtStorageElement);
                std::swap(curReplica, lastReplica);
            }
            totalTransfersCreated += numCreated;
        }

        //std::cout<<"["<<now<<"]\nnumActive: "<<numActive<<"\nnumToCreate: "<<numToCreate<<"\ntotalCreated: "<<numToCreatePerRSE<<std::endl;
        //mTransferMgr->mTransferLog << 1 << "|" << now << "|" << numToCreate << "|";
        mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
        mNextCallTick = now + mTickFreq;
    }
};
class CTransferGeneratorExponential : public CScheduleable
{
private:
    IBaseSim* mSim;
    CTransferManager* mTransferMgr;
    std::uint32_t mTickFreq;

public:
    //std::ofstream* mTransferLog;
    std::unique_ptr<CTransferNumberGenerator> mTransferNumberGen;
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<CStorageElement*> mDstStorageElements;

    CTransferGeneratorExponential(IBaseSim* sim, CTransferManager* transferMgr, const std::uint32_t tickFreq, const TickType startTick=0)
        : CScheduleable(startTick),
          mSim(sim),
          mTransferMgr(transferMgr),
          mTickFreq(tickFreq),
          mTransferNumberGen(new CTransferNumberGenerator(1, 1, tickFreq, 0.075))
    {}

    void OnUpdate(const TickType now) final
    {
        auto curRealtime = std::chrono::high_resolution_clock::now();

        const std::size_t numSrcStorageElements = mSrcStorageElements.size();
        const std::size_t numDstStorageElements = mDstStorageElements.size();
        assert(numSrcStorageElements > 0 && numDstStorageElements > 0);

        IBaseSim::RNGEngineType& rngEngine = mSim->mRNGEngine;
        std::exponential_distribution<double> dstStorageElementRndSelecter(0.25);

        const std::uint32_t numActive = static_cast<std::uint32_t>(mTransferMgr->GetNumActiveTransfers());
        const std::uint32_t numToCreate = mTransferNumberGen->GetNumToCreate(rngEngine, numActive, now);

        std::uint32_t totalTransfersCreated = 0;

        for(totalTransfersCreated=0; totalTransfersCreated<numToCreate; ++totalTransfersCreated)
        {
            CStorageElement* const dstStorageElement = mDstStorageElements[static_cast<std::size_t>(dstStorageElementRndSelecter(rngEngine) * 2) % numDstStorageElements];
            bool wasTransferCreated = false;
            for(std::size_t numSrcStorageElementsTried = 0; numSrcStorageElementsTried < numSrcStorageElements; ++numSrcStorageElementsTried)
            {
                std::uniform_int_distribution<std::size_t> srcStorageElementRndSelecter(numSrcStorageElementsTried, numSrcStorageElements - 1);
                CStorageElement*& srcStorageElement = mSrcStorageElements[srcStorageElementRndSelecter(rngEngine)];
                const std::size_t numSrcReplicas = srcStorageElement->mReplicas.size();
                for(std::size_t numSrcReplciasTried = 0; numSrcReplciasTried < numSrcReplicas; ++numSrcReplciasTried)
                {
                    std::uniform_int_distribution<std::size_t> srcReplicaRndSelecter(numSrcReplciasTried, numSrcReplicas - 1);
                    SReplica*& curReplica = srcStorageElement->mReplicas[srcReplicaRndSelecter(rngEngine)];
                    if(curReplica->IsComplete())
                    {
                        SReplica* const newReplica = dstStorageElement->CreateReplica(curReplica->GetFile());
                        if(newReplica != nullptr)
                        {
                            mTransferMgr->CreateTransfer(curReplica, newReplica, now);
                            wasTransferCreated = true;
                            break;
                        }
                    }
                    SReplica*& firstReplica = srcStorageElement->mReplicas.front();
                    std::swap(curReplica->mIndexAtStorageElement, firstReplica->mIndexAtStorageElement);
                    std::swap(curReplica, firstReplica);
                }
                if(wasTransferCreated)
                    break;
                std::swap(srcStorageElement, mSrcStorageElements.front());
            }
        }
        //std::cout<<"["<<now<<"]: numActive: "<<numActive<<"; numToCreate: "<<numToCreate<<std::endl;

        //mTransferMgr->mTransferLog << 1 << "|" << now << "|" << numToCreate << "|";
        mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
        mNextCallTick = now + mTickFreq;
    }
};
class CHearbeat : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mG2CTransferMgr;
    std::shared_ptr<CTransferManager> mC2CTransferMgr;
    std::uint32_t mTickFreq;

    std::chrono::high_resolution_clock::time_point mTimeLastUpdate;

public:
    std::unordered_map<std::string, std::chrono::duration<double>*> mProccessDurations;

    CHearbeat(IBaseSim* sim, std::shared_ptr<CTransferManager> g2cTransferMgr, std::shared_ptr<CTransferManager> c2cTransferMgr, const std::uint32_t tickFreq, const TickType startTick=0)
        : CScheduleable(startTick),
          mSim(sim),
          mG2CTransferMgr(g2cTransferMgr),
          mC2CTransferMgr(c2cTransferMgr),
          mTickFreq(tickFreq)
    {
        mTimeLastUpdate = std::chrono::high_resolution_clock::now();
    }


    void OnUpdate(const TickType now) final
    {
        auto curRealtime = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> timeDiff = curRealtime - mTimeLastUpdate;
        mUpdateDurationSummed += timeDiff;
        mTimeLastUpdate = curRealtime;

        std::stringstream statusOutput;
        statusOutput << std::fixed << std::setprecision(2);

        statusOutput << "[" << std::setw(6) << static_cast<std::uint32_t>(now / 1000) << "k]: ";
        statusOutput << "Runtime: " << mUpdateDurationSummed.count() << "s; ";
        statusOutput << "numFiles: " << mSim->mRucio->mFiles.size() / 1000.0 << "k; ";

        statusOutput << "activeTransfers: " << mG2CTransferMgr->GetNumActiveTransfers();
        statusOutput << " + " << mC2CTransferMgr->GetNumActiveTransfers() << "; ";

        statusOutput << "CompletedTransfers: " << mG2CTransferMgr->mNumCompletedTransfers;
        statusOutput << " + " << mC2CTransferMgr->mNumCompletedTransfers << "; ";

        statusOutput << "AvgTransferDuration: " << (mG2CTransferMgr->mSummedTransferDuration / mG2CTransferMgr->mNumCompletedTransfers);
        statusOutput << " + " << (mC2CTransferMgr->mSummedTransferDuration / mC2CTransferMgr->mNumCompletedTransfers) << std::endl;

       // mG2CTransferMgr->mNumCompletedTransfers = 0;
        mG2CTransferMgr->mSummedTransferDuration = 0;
        //mC2CTransferMgr->mNumCompletedTransfers = 0;
        mC2CTransferMgr->mSummedTransferDuration = 0;

		std::size_t maxW = 0;
		for (auto it : mProccessDurations)
			if (it.first.size() > maxW)
				maxW = it.first.size();

        statusOutput << "  " << std::setw(maxW) << "Duration" << ": " << std::setw(7) << timeDiff.count() << "s\n";
        for(auto it : mProccessDurations)
        {
            statusOutput << "  " << std::setw(maxW) << it.first;
            statusOutput << ": " << std::setw(6) << it.second->count();
            statusOutput << "s ("<< std::setw(5) << (it.second->count() / timeDiff.count()) * 100 << "%)\n";
            *(it.second) = std::chrono::duration<double>::zero();
        }
        std::cout << statusOutput.str() << std::endl;

        mNextCallTick = now + mTickFreq;
    }
};

bool InsertSite(ISite* site)
{
    std::stringstream row;
    row << site->GetId() << ",'"
        << site->GetName() << "','"
        << site->GetLocationName() << "'";
    return COutput::GetRef().InsertRow("Sites", row.str());
}
bool InsertStorageElement(CStorageElement* storageElement)
{
    std::stringstream row;
    row << storageElement->GetId() << ","
        << storageElement->GetSite()->GetId() << ",'"
        << storageElement->GetName() << "'";
    return COutput::GetRef().InsertRow("StorageElements", row.str());
}
bool InsertLinkSelector(CLinkSelector* linkselector)
{
    std::stringstream row;
    row << linkselector->GetId() << ","
        << linkselector->GetSrcSiteId() << ","
        << linkselector->GetDstSiteId();
    return COutput::GetRef().InsertRow("LinkSelectors", row.str());
}

void CSimpleSim::SetupDefaults()
{
    mRucio.reset(new CRucio);

    COutput& output = COutput::GetRef();

    std::stringstream columns;
    bool ok = false;

    ok = output.CreateTable("Sites", "id BIGINT PRIMARY KEY, name varchar(64), locationName varchar(64)");
    assert(ok);

    ok = output.CreateTable("StorageElements", "id BIGINT PRIMARY KEY, siteId BIGINT, name varchar(64), FOREIGN KEY(siteId) REFERENCES Sites(id)");
    assert(ok);

    ok = output.CreateTable("LinkSelectors", "id BIGINT PRIMARY KEY, srcSiteId BIGINT, dstSiteId BIGINT, FOREIGN KEY(srcSiteId) REFERENCES Sites(id), FOREIGN KEY(dstSiteId) REFERENCES Sites(id)");
    assert(ok);

    ok = output.CreateTable("Files", "id BIGINT PRIMARY KEY, createdAt BIGINT, lifetime BIGINT, filesize INTEGER");
    assert(ok);

    columns << "id BIGINT PRIMARY KEY,"
            << "fileId BIGINT,"
            << "srcStorageElementId BIGINT,"
            << "dstStorageElementId BIGINT,"
            << "startTick BIGINT,"
            << "endTick BIGINT,"
            << "FOREIGN KEY(fileId) REFERENCES Files(id),"
            << "FOREIGN KEY(srcStorageElementId) REFERENCES StorageElements(id),"
            << "FOREIGN KEY(dstStorageElementId) REFERENCES StorageElements(id)";
    ok = output.CreateTable("Transfers", columns.str());
    assert(ok);
    //INSERT INTO Transfers VALUES(?, ?, ?, ?, ?, ?);

    //proccesses
    std::vector<std::shared_ptr<CBillingGenerator>> billingGenerators;
    std::shared_ptr<CDataGenerator> dataGen(new CDataGenerator(this, 50, 0));
    std::shared_ptr<CReaper> reaper(new CReaper(mRucio.get(), 600, 600));


    std::shared_ptr<CTransferManager> g2cTransferMgr(new CTransferManager("g2c_transfers.dat", "g2c_traffic.dat", 20, 100));
    //std::shared_ptr<CTransferGeneratorUniform> g2cTransferGen(new CTransferGeneratorUniform(this, g2cTransferMgr.get(), 25));
    std::shared_ptr<CTransferGeneratorExponential> g2cTransferGen(new CTransferGeneratorExponential(this, g2cTransferMgr.get(), 25));
    g2cTransferGen->mTransferNumberGen->mSoftmaxScale = 15;
    g2cTransferGen->mTransferNumberGen->mSoftmaxOffset = 500;

    std::shared_ptr<CTransferManager> c2cTransferMgr(new CTransferManager("c2c_transfers.dat", "c2c_traffic.dat", 20, 100));
    std::shared_ptr<CTransferGeneratorExponential> c2cTransferGen(new CTransferGeneratorExponential(this, c2cTransferMgr.get(), 25));
    c2cTransferGen->mTransferNumberGen->mSoftmaxScale = 10;
    c2cTransferGen->mTransferNumberGen->mSoftmaxOffset = 40;

    std::shared_ptr<CHearbeat> heartbeat(new CHearbeat(this, g2cTransferMgr, c2cTransferMgr, 10000, 10000));
    heartbeat->mProccessDurations["DataGen"] = &(dataGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["G2CTransferUpdate"] = &(g2cTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["G2CTransferGen"] = &(g2cTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["C2CTransferUpdate"] = &(c2cTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["C2CTransferGen"] = &(c2cTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["Reaper"] = &(reaper->mUpdateDurationSummed);

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
                gridSite->CreateLinkSelector(region, ONE_GiB / 32);
                region->CreateLinkSelector(gridSite.get(), ONE_GiB / 128);
                for (auto& bucket : region->mStorageElements)
                    g2cTransferGen->mDstStorageElements.push_back(bucket.get());
            }
        }
        mSchedule.emplace(new CBillingGenerator(this));
    }
    g2cTransferGen->mSrcStorageElements = gridStoragleElements;
    dataGen->mStorageElements = gridStoragleElements;

    c2cTransferGen->mSrcStorageElements = g2cTransferGen->mDstStorageElements;
    c2cTransferGen->mDstStorageElements = g2cTransferGen->mDstStorageElements;

    for(auto& gridSite : mRucio->mGridSites)
    {
        ok = InsertSite(gridSite.get());
        assert(ok);
        for(auto& storageElement : gridSite->mStorageElements)
        {
            ok = InsertStorageElement(storageElement.get());
            assert(ok);
        }
        for(auto& linkselector : gridSite->mLinkSelectors)
        {
            ok = InsertLinkSelector(linkselector.get());
            assert(ok);
        }
    }

    for(std::unique_ptr<ISite>& cloudSite : mClouds[0]->mRegions)
    {
        auto region = dynamic_cast<gcp::CRegion*>(cloudSite.get());
        assert(region);
        ok = InsertSite(region);
        assert(ok);
        for (std::unique_ptr<gcp::CBucket>& bucket : region->mStorageElements)
        {
            ok = InsertStorageElement(bucket.get());
            assert(ok);
        }
        for(std::unique_ptr<CLinkSelector>& linkselector : region->mLinkSelectors)
        {
            ok = InsertLinkSelector(linkselector.get());
            assert(ok);
        }
    }

    mSchedule.push(dataGen);
    mSchedule.push(reaper);
    mSchedule.push(g2cTransferMgr);
    mSchedule.push(g2cTransferGen);
    mSchedule.push(c2cTransferMgr);
    mSchedule.push(c2cTransferGen);
    mSchedule.push(heartbeat);
}
