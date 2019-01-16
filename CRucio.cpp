#include <algorithm>
#include <cassert>

#include "COutput.hpp"
#include "CRucio.hpp"

std::size_t CStorageElement::mOutputQueryIdx = 0;

SFile::SFile(const std::uint32_t size, const TickType expiresAt)
    : mId(GetNewId()),
      mSize(size),
      mExpiresAt(expiresAt)
{
    mReplicas.reserve(8);
}
void SFile::Remove(const TickType now)
{
    for(auto& replica : mReplicas)
        replica->Remove(now);
}


SReplica::SReplica(SFile* const file, CStorageElement* const storageElement, const std::size_t indexAtStorageElement)
    : mFile(file),
      mStorageElement(storageElement),
      mIndexAtStorageElement(indexAtStorageElement)
{}
auto SReplica::Increase(std::uint32_t amount, const TickType now) -> std::uint32_t
{
    const std::uint32_t maxSize = mFile->GetSize();
    std::uint64_t newSize = mCurSize;
    newSize += amount;
    if (newSize > maxSize)
    {
        amount = maxSize - mCurSize;
        newSize = maxSize;
    }
    mCurSize = static_cast<std::uint32_t>(newSize);
    mStorageElement->OnIncreaseReplica(amount, now);
    return amount;
}
void SReplica::Remove(const TickType now)
{
    if(mTransferRef)
        (*mTransferRef) = nullptr;
    mStorageElement->OnRemoveReplica(this, now);
}



ISite::ISite(std::string&& name, std::string&& locationName)
	: mId(GetNewId()),
      mName(std::move(name)),
	  mLocationName(std::move(locationName))
{}
auto ISite::CreateLinkSelector(const ISite* const dstSite, const std::uint32_t bandwidth) -> CLinkSelector*
{
    auto result = mDstSiteIdToLinkSelectorIdx.insert({dstSite->mId, mLinkSelectors.size()});
    assert(result.second);
    CLinkSelector* newLinkSelector = new CLinkSelector(bandwidth, mId, dstSite->mId);
    mLinkSelectors.emplace_back(newLinkSelector);
    return newLinkSelector;
}
auto ISite::GetLinkSelector(const ISite* const dstSite) -> CLinkSelector*
{
    auto result = mDstSiteIdToLinkSelectorIdx.find(dstSite->mId);
    if(result == mDstSiteIdToLinkSelectorIdx.end())
        return nullptr;
    return mLinkSelectors[result->second].get();
}


CGridSite::CGridSite(std::string&& name, std::string&& locationName)
	: ISite(std::move(name), std::move(locationName))
{
	mStorageElements.reserve(8);
}
auto CGridSite::CreateStorageElement(std::string&& name) -> CStorageElement*
{
    CStorageElement* newStorageElement = new CStorageElement(std::move(name), this);
    mStorageElements.emplace_back(newStorageElement);
    return newStorageElement;
}



CStorageElement::CStorageElement(std::string&& name, ISite* const site)
	: mId(GetNewId()),
      mName(std::move(name)),
	  mSite(site)
{
	mFileIds.reserve(50000);
	mReplicas.reserve(50000);
}
auto CStorageElement::CreateReplica(SFile* const file) -> SReplica*
{
    //const auto result = file->mStorageElements.insert(this);
    const auto result = mFileIds.insert(file->GetId());

    if (!result.second)
        return nullptr;

    SReplica* newReplica = new SReplica(file, this, mReplicas.size());
    file->mReplicas.emplace_back(newReplica);
    mReplicas.push_back(newReplica);

    std::shared_ptr<CInsertStatements> outputs(new CInsertStatements(mOutputQueryIdx, 2));
    outputs->AddValue(file->GetId());
    outputs->AddValue(mId);
    COutput::GetRef().QueueInserts(outputs);

    return newReplica;
}

void CStorageElement::OnIncreaseReplica(const std::uint64_t amount, const TickType now)
{
    mUsedStorage += amount;
}

void CStorageElement::OnRemoveReplica(const SReplica* const replica, const TickType now)
{
    const auto FileIdIterator = mFileIds.find(replica->GetFile()->GetId());
    const std::size_t idxToDelete = replica->mIndexAtStorageElement;
    const std::uint32_t curSize = replica->GetCurSize();

    assert(FileIdIterator != mFileIds.cend());
    assert(idxToDelete < mReplicas.size());
    assert(curSize <= mUsedStorage);

    auto& lastReplica = mReplicas.back();
    std::size_t& idxLastReplica = lastReplica->mIndexAtStorageElement;

    mUsedStorage -= curSize;
    mFileIds.erase(FileIdIterator);
    if(idxToDelete != idxLastReplica)
    {
        idxLastReplica = idxToDelete;
        mReplicas[idxToDelete] = lastReplica;
    }
    mReplicas.pop_back();
}



CRucio::CRucio()
{
    mFiles.reserve(150000);
}
auto CRucio::CreateFile(const std::uint32_t size, const TickType expiresAt) -> SFile*
{
    SFile* newFile = new SFile(size, expiresAt);
    mFiles.emplace_back(newFile);
    return newFile;
}
auto CRucio::CreateGridSite(std::string&& name, std::string&& locationName) -> CGridSite*
{
    CGridSite* newSite = new CGridSite(std::move(name), std::move(locationName));
    mGridSites.emplace_back(newSite);
    return newSite;
}
auto CRucio::RunReaper(const TickType now) -> std::size_t
{
    const std::size_t numFiles = mFiles.size();

    if(numFiles == 0)
        return 0;

    std::size_t frontIdx = 0;
    std::size_t backIdx = numFiles - 1;

    while(backIdx > frontIdx && mFiles[backIdx]->mExpiresAt <= now)
    {
        mFiles[backIdx]->Remove(now);
        mFiles.pop_back();
        --backIdx;
    }

    for(;frontIdx < backIdx; ++frontIdx)
    {
        if(mFiles[frontIdx]->mExpiresAt <= now)
        {
            std::swap(mFiles[frontIdx], mFiles[backIdx]);
            do
            {
                mFiles[backIdx]->Remove(now);
                mFiles.pop_back();
                --backIdx;
            } while(backIdx > frontIdx && mFiles[backIdx]->mExpiresAt <= now);
        }
    }

    if(backIdx == 0 && mFiles.back()->mExpiresAt <= now)
    {
        mFiles[backIdx]->Remove(now);
        mFiles.pop_back();
    }
    return numFiles - mFiles.size();
}
