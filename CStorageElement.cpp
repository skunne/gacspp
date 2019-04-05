#include <cassert>

#include "ISite.hpp"

#include "CStorageElement.hpp"
#include "SFile.hpp"



std::size_t CStorageElement::mOutputQueryIdx = 0;


CStorageElement::CStorageElement(std::string&& name, ISite* const site)
	: mId(GetNewId()),
      mName(std::move(name)),
	  mSite(site)
{
	mFileIds.reserve(50000);
	mReplicas.reserve(50000);
}

auto CStorageElement::CreateReplica(SFile* const file) -> std::shared_ptr<SReplica>
{
    const auto result = mFileIds.insert(file->GetId());

    if (!result.second)
        return nullptr;

    auto newReplica = std::make_shared<SReplica>(file, this, mReplicas.size());
    file->mReplicas.emplace_back(newReplica);
    mReplicas.emplace_back(newReplica);

    return newReplica;
}

void CStorageElement::OnIncreaseReplica(const std::uint64_t amount, const TickType now)
{
    (void)now;
    mUsedStorage += amount;
}

void CStorageElement::OnRemoveReplica(const SReplica* const replica, const TickType now)
{
    (void)now;
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
