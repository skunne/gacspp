#include <cassert>

#include "ISite.hpp"

#include "COutput.hpp"
#include "CStorageElement.hpp"
#include "SFile.hpp"



std::size_t CStorageElement::mOutputQueryIdx = 0;


CStorageElement::CStorageElement(std::string&& name, ISite* const site)
	: mId(GetNewId()),
      mName(std::move(name)),
	  mSite(site)
{}

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

void CStorageElement::OnRemoveReplica(const SReplica* const replica, const TickType now, bool needLock)
{
    (void)now;
    const std::uint32_t curSize = replica->GetCurSize();

    std::unique_lock<std::mutex> lock(mReplicaRemoveMutex, std::defer_lock);
    if(needLock)
        lock.lock();

    const std::size_t idxToDelete = replica->mIndexAtStorageElement;
    auto& lastReplica = mReplicas.back();
    auto ret = mFileIds.erase(replica->GetFile()->GetId());
    assert(ret == 1);
    assert(idxToDelete < mReplicas.size());
    assert(curSize <= mUsedStorage);

    mUsedStorage -= curSize;

    std::size_t& idxLastReplica = lastReplica->mIndexAtStorageElement;
    if(idxToDelete != idxLastReplica)
    {
        idxLastReplica = idxToDelete;
        mReplicas[idxToDelete] = lastReplica;
    }
    mReplicas.pop_back();
}
