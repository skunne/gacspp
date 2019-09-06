#include <algorithm>
#include <cassert>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>

#include "IBaseCloud.hpp"
#include "IBaseSim.hpp"

#include "CLinkSelector.hpp"
#include "CRucio.hpp"
#include "COutput.hpp"
#include "CStorageElement.hpp"
#include "CommonScheduleables.hpp"
#include "SFile.hpp"


CDataGenerator::CDataGenerator(IBaseSim* sim, const std::uint32_t tickFreq, const TickType startTick)
    : CScheduleable(startTick),
      mSim(sim),
      mTickFreq(tickFreq)
{
    //mOutputQueryIdx = COutput::GetRef().AddPreparedSQLStatement("INSERT INTO Files VALUES(?, ?, ?, ?);");
    mOutputQueryIdx = COutput::GetRef().AddPreparedSQLStatement("COPY Files(id, createdAt, expiredAt, filesize) FROM STDIN with(FORMAT csv);", 4);
}

void CDataGenerator::OnUpdate(const TickType now)
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

auto CDataGenerator::GetRandomFileSize() -> std::uint32_t
{
    const double min = 64 * ONE_MiB;
    const double max = static_cast<double>(std::numeric_limits<std::uint32_t>::max());
    const double val = GiB_TO_BYTES(std::abs(mFileSizeRNG(mSim->mRNGEngine)));
    return static_cast<std::uint32_t>(std::clamp(val, min, max));
}

auto CDataGenerator::GetRandomNumFilesToGenerate() -> std::uint32_t
{
    return static_cast<std::uint32_t>( std::max(1.f, mNumFilesRNG(mSim->mRNGEngine)) );
}

auto CDataGenerator::GetRandomLifeTime() -> TickType
{
    float val = DAYS_TO_SECONDS(std::abs(mFileLifetimeRNG(mSim->mRNGEngine)));
    return static_cast<TickType>( std::max(float(SECONDS_PER_DAY), val) );
}

auto CDataGenerator::CreateFilesAndReplicas(const std::uint32_t numFiles, const std::uint32_t numReplicasPerFile, const TickType now) -> std::uint64_t
{
    if(numFiles == 0 || numReplicasPerFile == 0)
        return 0;

    const std::uint32_t numStorageElements = static_cast<std::uint32_t>( mStorageElements.size() );

    assert(numReplicasPerFile <= numStorageElements);

    auto fileInsertStmts = std::make_unique<CInsertStatements>(mOutputQueryIdx, numFiles * 4);
    auto replicaInsertStmts = std::make_unique<CInsertStatements>(CStorageElement::mOutputQueryIdx, numFiles * numReplicasPerFile * 2);
    std::uniform_int_distribution<std::uint32_t> rngSampler(0, numStorageElements);
    std::uint64_t bytesOfFilesGen = 0;
    for(std::uint32_t i = 0; i < numFiles; ++i)
    {
        const std::uint32_t fileSize = GetRandomFileSize();
        const TickType lifetime = GetRandomLifeTime();

        SFile* const file = mSim->mRucio->CreateFile(fileSize, now + lifetime);

        fileInsertStmts->AddValue(file->GetId());
        fileInsertStmts->AddValue(now);
        fileInsertStmts->AddValue(now + lifetime);
        fileInsertStmts->AddValue(fileSize);

        bytesOfFilesGen += fileSize;

        auto reverseRSEIt = mStorageElements.rbegin();
        //numReplicasPerFile <= numStorageElements !
        for(std::uint32_t numCreated = 0; numCreated<numReplicasPerFile; ++numCreated)
        {
            auto selectedElementIt = mStorageElements.begin() + (rngSampler(mSim->mRNGEngine) % (numStorageElements - numCreated));
            auto r = (*selectedElementIt)->CreateReplica(file);
            r->Increase(fileSize, now);
            r->mExpiresAt = now + (lifetime / numReplicasPerFile);
            replicaInsertStmts->AddValue(r->GetId());
            replicaInsertStmts->AddValue(file->GetId());
            replicaInsertStmts->AddValue((*selectedElementIt)->GetId());
            replicaInsertStmts->AddValue(now);
            replicaInsertStmts->AddValue(r->mExpiresAt);
            std::iter_swap(selectedElementIt, reverseRSEIt);
			++reverseRSEIt;
        }
    }
    COutput::GetRef().QueueInserts(std::move(fileInsertStmts));
    COutput::GetRef().QueueInserts(std::move(replicaInsertStmts));
    return bytesOfFilesGen;
}



CReaper::CReaper(CRucio *rucio, const std::uint32_t tickFreq, const TickType startTick)
    : CScheduleable(startTick),
      mRucio(rucio),
      mTickFreq(tickFreq)
{}

void CReaper::OnUpdate(const TickType now)
{
    auto curRealtime = std::chrono::high_resolution_clock::now();
    mRucio->RunReaper(now);

    mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
    mNextCallTick = now + mTickFreq;
}



CBillingGenerator::CBillingGenerator(IBaseSim* sim, const std::uint32_t tickFreq, const TickType startTick)
    : CScheduleable(startTick),
      mSim(sim),
      mTickFreq(tickFreq)
{}

void CBillingGenerator::OnUpdate(const TickType now)
{
    std::stringstream summary;
    const std::string caption = std::string(10, '=') + " Monthly Summary " + std::string(10, '=');

    summary << std::fixed << std::setprecision(2);

    summary << std::endl;
    summary << std::string(caption.length(), '=') << std::endl;
    summary << caption << std::endl;
    summary << std::string(caption.length(), '=') << std::endl;

    summary << "-Grid2Cloud Link Stats-" << std::endl;
    for(auto& srcGridSite : mSim->mRucio->mGridSites)
    {
        summary << srcGridSite->GetName() << std::endl;
        for (auto& dstRegion : mSim->mClouds[0]->mRegions)
        {
            auto link = srcGridSite->GetLinkSelector(dstRegion.get());
            if (link == nullptr)
                continue;
            summary << "\t--> " << dstRegion->GetName() << ": " << link->mDoneTransfers << std::endl;
            link->mDoneTransfers = 0;
        }
    }
    for(auto& cloud : mSim->mClouds)
    {
        summary << std::endl;
        summary<<cloud->GetName()<<" - Billing for Month "<<SECONDS_TO_MONTHS(now)<<":\n";
        auto res = cloud->ProcessBilling(now);
        summary << "\tStorage: " << res.first << " CHF" << std::endl;
        summary << "\tNetwork: " << res.second.first << " CHF" << std::endl;
        summary << "\tNetwork: " << res.second.second << " GiB" << std::endl;
    }

    summary << std::string(caption.length(), '=') << std::endl;
    std::cout << summary.str() << std::endl;

    mNextCallTick = now + mTickFreq;
}



CTransferManager::STransfer::STransfer( std::shared_ptr<SReplica> srcReplica,
                                        std::shared_ptr<SReplica> dstReplica,
                                        CLinkSelector* const linkSelector,
                                        const TickType startTick)
    : mSrcReplica(srcReplica),
      mDstReplica(dstReplica),
      mLinkSelector(linkSelector),
      mStartTick(startTick)
{}

CTransferManager::CTransferManager(const std::uint32_t tickFreq, const TickType startTick)
    : CScheduleable(startTick),
      mTickFreq(tickFreq)
{
    mActiveTransfers.reserve(1024*1024);

    //mOutputQueryIdx = COutput::GetRef().AddPreparedSQLStatement("INSERT INTO Transfers VALUES(?, ?, ?, ?, ?);");
    mOutputQueryIdx = COutput::GetRef().AddPreparedSQLStatement("COPY Transfers(id, srcReplicaId, dstReplicaId, startTick, endTick) FROM STDIN with(FORMAT csv);", 5);
}

void CTransferManager::CreateTransfer(std::shared_ptr<SReplica> srcReplica, std::shared_ptr<SReplica> dstReplica, const TickType now)
{
    assert(mActiveTransfers.size() < mActiveTransfers.capacity());

    ISite* const srcSite = srcReplica->GetStorageElement()->GetSite();
    ISite* const dstSite = dstReplica->GetStorageElement()->GetSite();
    CLinkSelector* const linkSelector = srcSite->GetLinkSelector(dstSite);

    linkSelector->mNumActiveTransfers += 1;
    mActiveTransfers.emplace_back(srcReplica, dstReplica, linkSelector, now);
}

void CTransferManager::OnUpdate(const TickType now)
{
    auto curRealtime = std::chrono::high_resolution_clock::now();
	const std::uint32_t timeDiff = static_cast<std::uint32_t>(now - mLastUpdated);
    mLastUpdated = now;

    std::size_t idx = 0;
    std::uint64_t summedTraffic = 0;
    auto outputs = std::make_unique<CInsertStatements>(mOutputQueryIdx, 6 + mActiveTransfers.size());

    while (idx < mActiveTransfers.size())
    {
        STransfer& transfer = mActiveTransfers[idx];
        std::shared_ptr<SReplica> srcReplica = transfer.mSrcReplica.lock();
        std::shared_ptr<SReplica> dstReplica = transfer.mDstReplica.lock();
        CLinkSelector* const linkSelector = transfer.mLinkSelector;

        if(!srcReplica || !dstReplica)
        {
            linkSelector->mFailedTransfers += 1;
            linkSelector->mNumActiveTransfers -= 1;
            transfer = std::move(mActiveTransfers.back());
            mActiveTransfers.pop_back();
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
            outputs->AddValue(srcReplica->GetId());
            outputs->AddValue(dstReplica->GetId());
            outputs->AddValue(transfer.mStartTick);
            outputs->AddValue(now);

            ++mNumCompletedTransfers;
            mSummedTransferDuration += now - transfer.mStartTick;

            linkSelector->mDoneTransfers += 1;
            linkSelector->mNumActiveTransfers -= 1;
            transfer = std::move(mActiveTransfers.back());
            mActiveTransfers.pop_back();
            continue; // handle same idx again
        }
        ++idx;
    }

    COutput::GetRef().QueueInserts(std::move(outputs));

    mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
    mNextCallTick = now + mTickFreq;
}



CFixedTimeTransferManager::STransfer::STransfer( std::shared_ptr<SReplica> srcReplica,
                                                 std::shared_ptr<SReplica> dstReplica,
                                                 CLinkSelector* const linkSelector,
                                                 const TickType startTick,
                                                 const std::uint32_t increasePerTick)
    : mSrcReplica(srcReplica),
      mDstReplica(dstReplica),
      mLinkSelector(linkSelector),
      mStartTick(startTick),
      mIncreasePerTick(increasePerTick)
{}

CFixedTimeTransferManager::CFixedTimeTransferManager(const std::uint32_t tickFreq, const TickType startTick)
    : CScheduleable(startTick),
      mTickFreq(tickFreq)
{
    mActiveTransfers.reserve(1024*1024);
    //mOutputQueryIdx = COutput::GetRef().AddPreparedSQLStatement("INSERT INTO Transfers VALUES(?, ?, ?, ?, ?);");
    mOutputQueryIdx = COutput::GetRef().AddPreparedSQLStatement("COPY Transfers(id, srcReplicaId, dstReplicaId, startTick, endTick) FROM STDIN with(FORMAT csv);", 5);
}

void CFixedTimeTransferManager::CreateTransfer(std::shared_ptr<SReplica> srcReplica, std::shared_ptr<SReplica> dstReplica, const TickType now, const TickType duration)
{
    assert(mActiveTransfers.size() < mActiveTransfers.capacity());

    ISite* const srcSite = srcReplica->GetStorageElement()->GetSite();
    ISite* const dstSite = dstReplica->GetStorageElement()->GetSite();
    CLinkSelector* const linkSelector = srcSite->GetLinkSelector(dstSite);

    std::uint32_t increasePerTick = static_cast<std::uint32_t>(static_cast<double>(srcReplica->GetFile()->GetSize()) / duration);

    linkSelector->mNumActiveTransfers += 1;
    mActiveTransfers.emplace_back(srcReplica, dstReplica, linkSelector, now, std::max(1U, increasePerTick));
}

void CFixedTimeTransferManager::OnUpdate(const TickType now)
{
    auto curRealtime = std::chrono::high_resolution_clock::now();
	const std::uint32_t timeDiff = static_cast<std::uint32_t>(now - mLastUpdated);
    mLastUpdated = now;

    std::size_t idx = 0;
    std::uint64_t summedTraffic = 0;
    auto outputs = std::make_unique<CInsertStatements>(mOutputQueryIdx, 6 + mActiveTransfers.size());

    while (idx < mActiveTransfers.size())
    {
        STransfer& transfer = mActiveTransfers[idx];
        std::shared_ptr<SReplica> srcReplica = transfer.mSrcReplica.lock();
        std::shared_ptr<SReplica> dstReplica = transfer.mDstReplica.lock();
        CLinkSelector* const linkSelector = transfer.mLinkSelector;

        if(!srcReplica || !dstReplica)
        {
            linkSelector->mFailedTransfers += 1;
            linkSelector->mNumActiveTransfers -= 1;
            transfer = std::move(mActiveTransfers.back());
            mActiveTransfers.pop_back();
            continue; // handle same idx again
        }

        std::uint32_t amount = dstReplica->Increase(transfer.mIncreasePerTick * timeDiff, now);
        summedTraffic += amount;
        linkSelector->mUsedTraffic += amount;

        if(dstReplica->IsComplete())
        {
            outputs->AddValue(GetNewId());
            outputs->AddValue(srcReplica->GetId());
            outputs->AddValue(dstReplica->GetId());
            outputs->AddValue(transfer.mStartTick);
            outputs->AddValue(now);

            ++mNumCompletedTransfers;
            mSummedTransferDuration += now - transfer.mStartTick;

            linkSelector->mDoneTransfers += 1;
            linkSelector->mNumActiveTransfers -= 1;
            transfer = std::move(mActiveTransfers.back());
            mActiveTransfers.pop_back();
            continue; // handle same idx again
        }
        ++idx;
    }

    COutput::GetRef().QueueInserts(std::move(outputs));

    mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
    mNextCallTick = now + mTickFreq;
}



CWavedTransferNumGen::CWavedTransferNumGen(const double softmaxScale, const double softmaxOffset, const std::uint32_t samplingFreq, const double baseFreq)
    : mSoftmaxScale(softmaxScale),
      mSoftmaxOffset(softmaxOffset),
      mAlpha(1.0/samplingFreq * PI/180.0 * baseFreq)
{}

auto CWavedTransferNumGen::GetNumToCreate(RNGEngineType& rngEngine, std::uint32_t numActive, const TickType now) -> std::uint32_t
{
    const double softmax = (std::cos(now * mAlpha) * mSoftmaxScale + mSoftmaxOffset) * (1.0 + mSoftmaxRNG(rngEngine) * 0.02);
    const double diffSoftmaxActive = softmax - numActive;
    if(diffSoftmaxActive < 0.5)
        return 0;
    return static_cast<std::uint32_t>(std::pow(diffSoftmaxActive, abs(mPeakinessRNG(rngEngine))));
}



CUniformTransferGen::CUniformTransferGen(IBaseSim* sim,
                                         std::shared_ptr<CTransferManager> transferMgr,
                                         std::shared_ptr<CBaseTransferNumGen> transferNumGen,
                                         const std::uint32_t tickFreq,
                                         const TickType startTick )
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq),
      mTransferNumGen(transferNumGen)
{}

void CUniformTransferGen::OnUpdate(const TickType now)
{
    assert(mSrcStorageElements.size() > 0);

    auto curRealtime = std::chrono::high_resolution_clock::now();

    RNGEngineType& rngEngine = mSim->mRNGEngine;
    std::uniform_int_distribution<std::size_t> dstStorageElementRndChooser(0, mDstStorageElements.size()-1);

    const std::uint32_t numActive = static_cast<std::uint32_t>(mTransferMgr->GetNumActiveTransfers());
    const std::uint32_t numToCreate = mTransferNumGen->GetNumToCreate(rngEngine, numActive, now);
    const std::uint32_t numToCreatePerRSE = static_cast<std::uint32_t>( numToCreate/static_cast<double>(mSrcStorageElements.size()) );

    auto replicaInsertStmts = std::make_unique<CInsertStatements>(CStorageElement::mOutputQueryIdx, numToCreate * 2);

    std::uint32_t totalTransfersCreated = 0;
    for(CStorageElement* srcStorageElement : mSrcStorageElements)
    {
        std::uint32_t numCreated = 0;
        std::size_t numSrcReplicas = srcStorageElement->mReplicas.size();
        std::uniform_int_distribution<std::size_t> rngSampler(0, numSrcReplicas);
        while(numSrcReplicas > 0 && numCreated < numToCreatePerRSE)
        {
            const std::size_t idx = rngSampler(rngEngine) % numSrcReplicas;
            --numSrcReplicas;

            std::shared_ptr<SReplica>& curReplica = srcStorageElement->mReplicas[idx];
            if(curReplica->IsComplete())
            {
                SFile* const file = curReplica->GetFile();
                CStorageElement* const dstStorageElement = mDstStorageElements[dstStorageElementRndChooser(rngEngine)];
                std::shared_ptr<SReplica> newReplica = dstStorageElement->CreateReplica(file);
                if(newReplica != nullptr)
                {
                    replicaInsertStmts->AddValue(newReplica->GetId());
                    replicaInsertStmts->AddValue(file->GetId());
                    replicaInsertStmts->AddValue(dstStorageElement->GetId());
                    replicaInsertStmts->AddValue(now);
                    replicaInsertStmts->AddValue(newReplica->mExpiresAt);
                    mTransferMgr->CreateTransfer(curReplica, newReplica, now);
                    ++numCreated;
                }
            }
            std::shared_ptr<SReplica>& lastReplica = srcStorageElement->mReplicas[numSrcReplicas];
            std::swap(curReplica->mIndexAtStorageElement, lastReplica->mIndexAtStorageElement);
            std::swap(curReplica, lastReplica);
        }
        totalTransfersCreated += numCreated;
    }

    COutput::GetRef().QueueInserts(std::move(replicaInsertStmts));

    //std::cout<<"["<<now<<"]\nnumActive: "<<numActive<<"\nnumToCreate: "<<numToCreate<<"\ntotalCreated: "<<numToCreatePerRSE<<std::endl;
    mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
    mNextCallTick = now + mTickFreq;
}



CExponentialTransferGen::CExponentialTransferGen(IBaseSim* sim,
                                                 std::shared_ptr<CTransferManager> transferMgr,
                                                 std::shared_ptr<CBaseTransferNumGen> transferNumGen,
                                                 const std::uint32_t tickFreq,
                                                 const TickType startTick )
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq),
      mTransferNumGen(transferNumGen)
{}

void CExponentialTransferGen::OnUpdate(const TickType now)
{
    auto curRealtime = std::chrono::high_resolution_clock::now();

    const std::size_t numSrcStorageElements = mSrcStorageElements.size();
    const std::size_t numDstStorageElements = mDstStorageElements.size();
    assert(numSrcStorageElements > 0 && numDstStorageElements > 0);

    RNGEngineType& rngEngine = mSim->mRNGEngine;
    std::exponential_distribution<double> dstStorageElementRndSelecter(0.25);

    const std::uint32_t numActive = static_cast<std::uint32_t>(mTransferMgr->GetNumActiveTransfers());
    const std::uint32_t numToCreate = mTransferNumGen->GetNumToCreate(rngEngine, numActive, now);

    auto replicaInsertStmts = std::make_unique<CInsertStatements>(CStorageElement::mOutputQueryIdx, numToCreate * 2);

    for(std::uint32_t totalTransfersCreated=0; totalTransfersCreated<numToCreate; ++totalTransfersCreated)
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
                std::shared_ptr<SReplica>& curReplica = srcStorageElement->mReplicas[srcReplicaRndSelecter(rngEngine)];
                if(curReplica->IsComplete())
                {
                    SFile* const file = curReplica->GetFile();
                    std::shared_ptr<SReplica> newReplica = dstStorageElement->CreateReplica(file);
                    if(newReplica != nullptr)
                    {
                        replicaInsertStmts->AddValue(newReplica->GetId());
                        replicaInsertStmts->AddValue(file->GetId());
                        replicaInsertStmts->AddValue(dstStorageElement->GetId());
                        replicaInsertStmts->AddValue(now);
                        replicaInsertStmts->AddValue(newReplica->mExpiresAt);
                        mTransferMgr->CreateTransfer(curReplica, newReplica, now);
                        wasTransferCreated = true;
                        break;
                    }
                }
                std::shared_ptr<SReplica>& firstReplica = srcStorageElement->mReplicas.front();
                std::swap(curReplica->mIndexAtStorageElement, firstReplica->mIndexAtStorageElement);
                std::swap(curReplica, firstReplica);
            }
            if(wasTransferCreated)
                break;
            std::swap(srcStorageElement, mSrcStorageElements.front());
        }
    }

    COutput::GetRef().QueueInserts(std::move(replicaInsertStmts));
    //std::cout<<"["<<now<<"]: numActive: "<<numActive<<"; numToCreate: "<<numToCreate<<std::endl;

    mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
    mNextCallTick = now + mTickFreq;
}



CSrcPrioTransferGen::CSrcPrioTransferGen(IBaseSim* sim,
                                         std::shared_ptr<CTransferManager> transferMgr,
                                         std::shared_ptr<CBaseTransferNumGen> transferNumGen,
                                         const std::uint32_t tickFreq,
                                         const TickType startTick )
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq),
      mTransferNumGen(transferNumGen)
{}

void CSrcPrioTransferGen::OnUpdate(const TickType now)
{
    auto curRealtime = std::chrono::high_resolution_clock::now();

    const std::vector<std::unique_ptr<SFile>>& allFiles = mSim->mRucio->mFiles;
    const std::size_t numDstStorageElements = mDstStorageElements.size();
    assert(allFiles.size() > 0 && numDstStorageElements > 0);

    RNGEngineType& rngEngine = mSim->mRNGEngine;
    std::exponential_distribution<double> dstStorageElementRndSelecter(0.25);
    std::uniform_int_distribution<std::size_t> fileRndSelector(0, allFiles.size() - 1);

    const std::uint32_t numActive = static_cast<std::uint32_t>(mTransferMgr->GetNumActiveTransfers());
    const std::uint32_t numToCreate = mTransferNumGen->GetNumToCreate(rngEngine, numActive, now);

    auto replicaInsertStmts = std::make_unique<CInsertStatements>(CStorageElement::mOutputQueryIdx, numToCreate * 2);
    std::uint32_t flexCreationLimit = numToCreate;
    for(std::uint32_t totalTransfersCreated=0; totalTransfersCreated< flexCreationLimit; ++totalTransfersCreated)
    {
        CStorageElement* const dstStorageElement = mDstStorageElements[static_cast<std::size_t>(dstStorageElementRndSelecter(rngEngine) * 2) % numDstStorageElements];
        SFile* fileToTransfer = allFiles[fileRndSelector(rngEngine)].get();

        for(std::uint32_t i = 0; i < 5 && fileToTransfer->mReplicas.empty() && fileToTransfer->mExpiresAt < (now + 100); ++i)
            fileToTransfer = allFiles[fileRndSelector(rngEngine)].get();

        const std::vector<std::shared_ptr<SReplica>>& replicas = fileToTransfer->mReplicas;
        if(replicas.empty())
        {
            flexCreationLimit += 1;
            continue;
        }

        std::shared_ptr<SReplica> newReplica = dstStorageElement->CreateReplica(fileToTransfer);
        if(newReplica != nullptr)
        {
            newReplica->mExpiresAt = now + SECONDS_PER_DAY;
            int minPrio = std::numeric_limits<int>::max();
            std::vector<std::shared_ptr<SReplica>> bestSrcReplicas;
            for(const std::shared_ptr<SReplica>& replica : replicas)
            {
                if(!replica->IsComplete())
                    continue; // at least the newly created one will be skipped

                const auto result = mSrcStorageElementIdToPrio.find(replica->GetStorageElement()->GetId());
                if(result != mSrcStorageElementIdToPrio.cend())
                {
                    if(result->second < minPrio)
                    {
                        minPrio = result->second;
                        bestSrcReplicas.clear();
                        bestSrcReplicas.emplace_back(replica);
                    }
                    else if(result->second == minPrio)
                        bestSrcReplicas.emplace_back(replica);
                }
            }

            if(bestSrcReplicas.empty())
            {
                flexCreationLimit += 1;
                continue;
            }

            std::shared_ptr<SReplica> bestSrcReplica = bestSrcReplicas[0];
            if (minPrio > 0)
            {
                const ISite* const dstSite = dstStorageElement->GetSite();
                double minWeight = std::numeric_limits<double>::max();
                for (std::shared_ptr<SReplica>& replica : bestSrcReplicas)
                {
                    double w = replica->GetStorageElement()->GetSite()->GetLinkSelector(dstSite)->GetWeight();
                    if (w < minWeight)
                    {
                        minWeight = w;
                        bestSrcReplica = replica;
                    }
                }
            }
            replicaInsertStmts->AddValue(newReplica->GetId());
            replicaInsertStmts->AddValue(fileToTransfer->GetId());
            replicaInsertStmts->AddValue(dstStorageElement->GetId());
            replicaInsertStmts->AddValue(now);
            replicaInsertStmts->AddValue(newReplica->mExpiresAt);

            mTransferMgr->CreateTransfer(bestSrcReplica, newReplica, now);
        }
        else
        {
            //replica already exists
        }
    }

    COutput::GetRef().QueueInserts(std::move(replicaInsertStmts));
    //std::cout<<"["<<now<<"]: numActive: "<<numActive<<"; numToCreate: "<<numToCreate<<std::endl;

    mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
    mNextCallTick = now + mTickFreq;
}



CJobSlotTransferGen::CJobSlotTransferGen(IBaseSim* sim,
                                         std::shared_ptr<CFixedTimeTransferManager> transferMgr,
                                         const std::uint32_t tickFreq,
                                         const TickType startTick )
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq)
{}

void CJobSlotTransferGen::OnUpdate(const TickType now)
{
    auto curRealtime = std::chrono::high_resolution_clock::now();

    const std::vector<std::unique_ptr<SFile>>& allFiles = mSim->mRucio->mFiles;
    assert(allFiles.size() > 0);

    RNGEngineType& rngEngine = mSim->mRNGEngine;
    std::uniform_int_distribution<std::size_t> fileRndSelector(0, allFiles.size() - 1);


    auto replicaInsertStmts = std::make_unique<CInsertStatements>(CStorageElement::mOutputQueryIdx, 512);
    for(auto& dstInfo : mDstInfo)
    {
        CStorageElement* const dstStorageElement = dstInfo.first;
        SJobSlotInfo& jobSlotInfo = dstInfo.second;

        auto& schedule = jobSlotInfo.mSchedule;
        const std::uint32_t numMaxSlots = jobSlotInfo.mNumMaxSlots;
        std::uint32_t usedSlots = 0;
        for(std::size_t idx=0; idx<schedule.size();)
        {
            if(schedule[idx].first <= now)
            {
                schedule[idx] = std::move(schedule.back());
                schedule.pop_back();
                continue;
            }
            usedSlots += schedule[idx].second;
            idx += 1;
        }
 
        assert(numMaxSlots >= usedSlots);

        // todo: consider mTickFreq
        std::uint32_t flexCreationLimit = std::min(numMaxSlots - usedSlots, std::uint32_t(1 + (0.01 * numMaxSlots)));
        std::pair<TickType, std::uint32_t> newJobs = std::make_pair(now+900, 0);
        for(std::uint32_t totalTransfersCreated=0; totalTransfersCreated<flexCreationLimit; ++totalTransfersCreated)
        {
            SFile* fileToTransfer = allFiles[fileRndSelector(rngEngine)].get();

            for(std::uint32_t i = 0; i < 10 && fileToTransfer->mReplicas.empty() && fileToTransfer->mExpiresAt < (now + 100); ++i)
                fileToTransfer = allFiles[fileRndSelector(rngEngine)].get();

            const std::vector<std::shared_ptr<SReplica>>& replicas = fileToTransfer->mReplicas;
            if(replicas.empty())
            {
                flexCreationLimit += 1;
                continue;
            }

            std::shared_ptr<SReplica> newReplica = dstStorageElement->CreateReplica(fileToTransfer);
            if(newReplica != nullptr)
            {
                newReplica->mExpiresAt = now + SECONDS_PER_DAY;
                int minPrio = std::numeric_limits<int>::max();
                std::vector<std::shared_ptr<SReplica>> bestSrcReplicas;
                for(const std::shared_ptr<SReplica>& replica : replicas)
                {
                    if(!replica->IsComplete())
                        continue; // at least the newly created one will be skipped

                    const auto result = mSrcStorageElementIdToPrio.find(replica->GetStorageElement()->GetId());
                    if(result != mSrcStorageElementIdToPrio.cend())
                    {
                        if(result->second < minPrio)
                        {
                            minPrio = result->second;
                            bestSrcReplicas.clear();
                            bestSrcReplicas.emplace_back(replica);
                        }
                        else if(result->second == minPrio)
                            bestSrcReplicas.emplace_back(replica);
                    }
                }

                if(bestSrcReplicas.empty())
                {
                    flexCreationLimit += 1;
                    continue;
                }

                std::shared_ptr<SReplica> bestSrcReplica = bestSrcReplicas[0];
                if (minPrio > 0)
                {
                    const ISite* const dstSite = dstStorageElement->GetSite();
                    double minWeight = std::numeric_limits<double>::max();
                    for (std::shared_ptr<SReplica>& replica : bestSrcReplicas)
                    {
                        double w = replica->GetStorageElement()->GetSite()->GetLinkSelector(dstSite)->GetWeight();
                        if (w < minWeight)
                        {
                            minWeight = w;
                            bestSrcReplica = replica;
                        }
                    }
                }
                replicaInsertStmts->AddValue(newReplica->GetId());
                replicaInsertStmts->AddValue(fileToTransfer->GetId());
                replicaInsertStmts->AddValue(dstStorageElement->GetId());
                replicaInsertStmts->AddValue(now);
                replicaInsertStmts->AddValue(newReplica->mExpiresAt);

                mTransferMgr->CreateTransfer(bestSrcReplica, newReplica, now, 60);
                newJobs.second += 1;
            }
            else
            {
                //replica already exists
            }
        }
        if(newJobs.second > 0)
            schedule.push_back(newJobs);
    }

    COutput::GetRef().QueueInserts(std::move(replicaInsertStmts));
    //std::cout<<"["<<now<<"]: numActive: "<<numActive<<"; numToCreate: "<<numToCreate<<std::endl;

    mUpdateDurationSummed += std::chrono::high_resolution_clock::now() - curRealtime;
    mNextCallTick = now + mTickFreq;
}



CHeartbeat::CHeartbeat(IBaseSim* sim, std::shared_ptr<CFixedTimeTransferManager> g2cTransferMgr, std::shared_ptr<CTransferManager> c2cTransferMgr, const std::uint32_t tickFreq, const TickType startTick)
    : CScheduleable(startTick),
      mSim(sim),
      mG2CTransferMgr(g2cTransferMgr),
      mC2CTransferMgr(c2cTransferMgr),
      mTickFreq(tickFreq)
{
    mTimeLastUpdate = std::chrono::high_resolution_clock::now();
}

void CHeartbeat::OnUpdate(const TickType now)
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
    if(mC2CTransferMgr)
        statusOutput << " + " << mC2CTransferMgr->GetNumActiveTransfers();
    statusOutput << "; ";

    statusOutput << "CompletedTransfers: " << mG2CTransferMgr->mNumCompletedTransfers;
    if(mC2CTransferMgr)
        statusOutput << " + " << mC2CTransferMgr->mNumCompletedTransfers;
    statusOutput << "; ";

    statusOutput << "AvgTransferDuration: " << (mG2CTransferMgr->mSummedTransferDuration / mG2CTransferMgr->mNumCompletedTransfers);
    if(mC2CTransferMgr)
        statusOutput << " + " << (mC2CTransferMgr->mSummedTransferDuration / mC2CTransferMgr->mNumCompletedTransfers);
    statusOutput << std::endl;

    mG2CTransferMgr->mNumCompletedTransfers = 0;
    mG2CTransferMgr->mSummedTransferDuration = 0;
    if(mC2CTransferMgr)
    {
        mC2CTransferMgr->mNumCompletedTransfers = 0;
        mC2CTransferMgr->mSummedTransferDuration = 0;
    }

	std::size_t maxW = 0;
	for (auto it : mProccessDurations)
		if (it.first.size() > maxW)
			maxW = it.first.size();

    statusOutput << "  " << std::setw(maxW) << "Duration" << ": " << std::setw(6) << timeDiff.count() << "s\n";
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
