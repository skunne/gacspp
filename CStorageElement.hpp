#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "constants.h"

class ISite;
struct SFile;
struct SReplica;



class CStorageElement
{
private:
    IdType mId;
    std::string mName;
	std::unordered_set<IdType> mFileIds;

protected:
    ISite* mSite;
    std::uint64_t mUsedStorage = 0;

public:
    static std::size_t mOutputQueryIdx;

	std::vector<std::shared_ptr<SReplica>> mReplicas;

	CStorageElement(std::string&& name, ISite* const site);
    CStorageElement(CStorageElement&&) = default;
    CStorageElement& operator=(CStorageElement&&) = default;

    CStorageElement(CStorageElement const&) = delete;
    CStorageElement& operator=(CStorageElement const&) = delete;

	auto CreateReplica(SFile* file) -> std::shared_ptr<SReplica>;

    virtual void OnIncreaseReplica(const std::uint64_t amount, const TickType now);
    virtual void OnRemoveReplica(const SReplica* replica, const TickType now);

	inline auto GetId() const -> IdType
	{return mId;}
    inline auto GetName() const -> const std::string&
    {return mName;}
    inline auto GetSite() const -> const ISite*
    {return mSite;}
    inline auto GetSite() -> ISite*
    {return mSite;}
};
