#include <algorithm>
#include <cassert>

#include "CRucio.hpp"


SFile::IdType SFile::IdCounter = 0;

SFile::SFile(std::uint32_t size, std::uint64_t expiresAt)
    : mId(++IdCounter),
      mSize(size),
      mExpiresAt(expiresAt)
{
	//mReplicas.reserve(4);
}
/*
auto SFile::CreateReplica(CStorageElement& storageElement) -> SReplica&
{
	const auto result = storageElement.mFileIds.insert(mId);
	assert(result.second == true);

	//const std::size_t idx = mReplicas.size();
	mReplicas.emplace_back(this, &storageElement);
	return mReplicas.back();
}
*/

SReplica::SReplica(SFile* file, CStorageElement* storageElement, std::size_t indexAtStorageElement)
    : mFile(file),
      mStorageElement(storageElement),
      mIndexAtStorageElement(indexAtStorageElement)
{}
auto SReplica::Increase(std::uint32_t amount, std::uint64_t now) -> std::uint32_t
{
    mCurSize = std::min(mCurSize + amount, mFile->GetSize());
    mStorageElement->OnIncreaseReplica(amount, now);
    return mCurSize;
}
void SReplica::Remove(std::uint64_t now)
{
    mStorageElement->OnRemoveReplica(*this, now);
}



ISite::ISite(std::string&& name, std::string&& locationName)
	: mName(std::move(name)),
	  mLocationName(std::move(locationName))
{}
auto ISite::CreateLinkSelector(ISite& dstSite, std::uint32_t bandwidth) -> CLinkSelector&
{
    mLinkSelectors.emplace_back(bandwidth);
    return mLinkSelectors.back();
}

CGridSite::CGridSite(std::string&& name, std::string&& locationName)
	: ISite(std::move(name), std::move(locationName))
{
	mStorageElements.reserve(16);
}
auto CGridSite::CreateStorageElement(std::string&& name) -> CStorageElement&
{
    mStorageElements.emplace_back(std::move(name), this);
    return mStorageElements.back();
}



CStorageElement::CStorageElement(std::string&& name, ISite* site)
	: mName(std::move(name)),
	  mSite(site)
{
	mFileIds.reserve(500000);
	mReplicas.reserve(500000);
}

auto CStorageElement::CreateReplica(SFile* file) -> SReplica&
{
	const auto result = mFileIds.insert(file->GetId());
	assert(result.second == true);

	const std::size_t idx = mReplicas.size();
	mReplicas.emplace_back(file, this, idx);
	return mReplicas.back();
}

void CStorageElement::OnIncreaseReplica(std::uint64_t amount, std::uint64_t now)
{
    mUsedStorage += amount;
}

void CStorageElement::OnRemoveReplica(const SReplica& replica, std::uint64_t now)
{
    const auto FileIdIterator = mFileIds.find(replica.GetFile()->GetId());
    const std::size_t idxToDelete = replica.mIndexAtStorageElement;
    const std::uint32_t curSize = replica.GetCurSize();

    assert(FileIdIterator != mFileIds.cend());
    assert(idxToDelete < mReplicas.size());
    assert(curSize <= mUsedStorage);

    SReplica& lastReplica = mReplicas.back();
    std::size_t& idxLastReplica = lastReplica.mIndexAtStorageElement;

    mFileIds.erase(FileIdIterator);
    mUsedStorage -= curSize;
    if(idxToDelete != idxLastReplica)
    {
        idxLastReplica = idxToDelete;
        mReplicas[idxToDelete] = std::move(lastReplica);
    }
    mReplicas.pop_back();
}



CRucio::CRucio()
{
    mFiles.reserve(500000);
}

auto CRucio::CreateFile(std::uint32_t size, std::uint64_t expiresAt) -> SFile&
{
    mFiles.emplace_back(size, expiresAt);
    return mFiles.back();
}
auto CRucio::CreateGridSite(std::string&& name, std::string&& locationName) -> CGridSite&
{
    mGridSites.emplace_back(std::move(name), std::move(locationName));
    return mGridSites.back();
}
auto CRucio::RunReaper(std::uint64_t now) -> std::size_t
{
    const std::size_t numFiles = mFiles.size();

    if(numFiles == 0)
        return 0;

    std::size_t frontIdx = 0;
    std::size_t backIdx = numFiles - 1;

    while(backIdx > frontIdx && mFiles[backIdx].mExpiresAt <= now)
    {
        mFiles.pop_back();
        --backIdx;
    }

    while(frontIdx < backIdx)
    {
        if(mFiles[frontIdx].mExpiresAt <= now)
        {
            mFiles[frontIdx] = std::move(mFiles[backIdx]);
            do
            {
                mFiles.pop_back();
                --backIdx;
            } while(backIdx > frontIdx && mFiles[backIdx].mExpiresAt <= now);
        }
        ++frontIdx;
    }

    if(backIdx == 0)
    {
        if(mFiles.back().mExpiresAt <= now)
            mFiles.pop_back();
    }

    return numFiles - mFiles.size();
}
