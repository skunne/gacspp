#include <algorithm>
#include <cassert>

#include "CRucio.hpp"


SFile::IdType SFile::IdCounter = 0;
ISite::IdType ISite::IdCounter = 0;

SFile::SFile(std::uint32_t size, std::uint64_t expiresAt)
    : mId(++IdCounter),
      mSize(size),
      mExpiresAt(expiresAt)
{
    mReplicas.reserve(8);
    mStorageElements.reserve(8);
}

SReplica::SReplica(SFile* file, CStorageElement* storageElement, std::size_t indexAtStorageElement)
    : mFile(file),
      mStorageElement(storageElement),
      mIndexAtStorageElement(indexAtStorageElement)
{}
auto SReplica::Increase(std::uint32_t amount, std::uint64_t now) -> std::uint32_t
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
void SReplica::Remove(std::uint64_t now)
{
    mStorageElement->OnRemoveReplica(this, now);
}



ISite::ISite(std::string&& name, std::string&& locationName)
	: mId(++IdCounter),
      mName(std::move(name)),
	  mLocationName(std::move(locationName))
{}
auto ISite::CreateLinkSelector(const ISite* dstSite, std::uint32_t bandwidth) -> CLinkSelector*
{
    auto result = mDstSiteIdToLinkSelectorIdx.insert({dstSite->mId, mLinkSelectors.size()});
    assert(result.second);
    CLinkSelector* newLinkSelector = new CLinkSelector(bandwidth);
    mLinkSelectors.emplace_back(newLinkSelector);
    return newLinkSelector;
}
auto ISite::GetLinkSelector(const ISite* dstSite) -> CLinkSelector*
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



CStorageElement::CStorageElement(std::string&& name, ISite* site)
	: mName(std::move(name)),
	  mSite(site)
{
	//mFileIds.reserve(100000);
	mReplicas.reserve(100000);
}
auto CStorageElement::CreateReplica(SFile* file) -> SReplica*
{
    const auto result = file->mStorageElements.insert(this);
    //const auto result = mFileIds.insert(file->GetId());

    if (!result.second)
        return nullptr;

    SReplica* newReplica = new SReplica(file, this, mReplicas.size());
    mReplicas.emplace_back(newReplica);
    file->mReplicas.push_back(newReplica);
    return newReplica;
}

void CStorageElement::OnIncreaseReplica(std::uint64_t amount, std::uint64_t now)
{
    mUsedStorage += amount;
}

void CStorageElement::OnRemoveReplica(const SReplica* replica, std::uint64_t now)
{
    //const auto FileIdIterator = mFileIds.find(replica->GetFile()->GetId());
    const std::size_t idxToDelete = replica->mIndexAtStorageElement;
    const std::uint32_t curSize = replica->GetCurSize();

    //assert(FileIdIterator != mFileIds.cend());
    assert(idxToDelete < mReplicas.size());
    assert(curSize <= mUsedStorage);

    auto& lastReplica = mReplicas.back();
    std::size_t& idxLastReplica = lastReplica->mIndexAtStorageElement;

    mUsedStorage -= curSize;
    //mFileIds.erase(FileIdIterator);
    if(idxToDelete != idxLastReplica)
    {
        idxLastReplica = idxToDelete;
        mReplicas[idxToDelete] = std::move(lastReplica);
    }
    mReplicas.pop_back();
}



CRucio::CRucio()
{
    mFiles.reserve(150000);
}

auto CRucio::CreateFile(std::uint32_t size, std::uint64_t expiresAt) -> SFile*
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
auto CRucio::RunReaper(std::uint64_t now) -> std::size_t
{
    const std::size_t numFiles = mFiles.size();

    if(numFiles == 0)
        return 0;

    std::size_t frontIdx = 0;
    std::size_t backIdx = numFiles - 1;

    while(backIdx > frontIdx && mFiles[backIdx]->mExpiresAt <= now)
    {
        for(auto replica : mFiles[backIdx]->mReplicas)
            replica->Remove(now);
        mFiles.pop_back();
        --backIdx;
    }

    while(frontIdx < backIdx)
    {
        if(mFiles[frontIdx]->mExpiresAt <= now)
        {
            std::swap(mFiles[frontIdx], mFiles[backIdx]);
            do
            {
                for(auto replica : mFiles[backIdx]->mReplicas)
                    replica->Remove(now);
                mFiles.pop_back();
                --backIdx;
            } while(backIdx > frontIdx && mFiles[backIdx]->mExpiresAt <= now);
        }
        ++frontIdx;
    }

    if(backIdx == 0)
    {
        if(mFiles.back()->mExpiresAt <= now)
        {
            for(auto replica : mFiles.back()->mReplicas)
                replica->Remove(now);
            mFiles.pop_back();
        }
    }

    return numFiles - mFiles.size();
}
