#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "constants.h"

class ISite;
class CStorageElement;
struct SReplica;

class CLinkSelector
{
private:
    //monitoring
    IdType mId;
    IdType mSrcSiteId;
    IdType mDstSiteId;
public:
    std::uint32_t mDoneTransfers = 0;
    std::uint32_t mFailedTransfers = 0;

public:
	typedef std::vector<std::pair<std::uint64_t, double>> PriceInfoType;

	CLinkSelector(const std::uint32_t bandwidth, const IdType srcSiteId, const IdType dstSiteId)
		: mId(GetNewId()),
          mSrcSiteId(srcSiteId),
          mDstSiteId(dstSiteId),
          mBandwidth(bandwidth)
	{}

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetSrcSiteId() const -> IdType
    {return mSrcSiteId;}
    inline auto GetDstSiteId() const -> IdType
    {return mDstSiteId;}

    inline auto GetWeight() const -> double
    { return mNetworkPrice.back().second; }

    PriceInfoType mNetworkPrice = {{0,0}};
    std::uint64_t mUsedTraffic = 0;
    std::uint32_t mNumActiveTransfers = 0;
    std::uint32_t mBandwidth;
};

struct SFile
{
private:
    IdType mId;
    std::uint32_t mSize;

public:
	std::vector<std::shared_ptr<SReplica>> mReplicas;

    TickType mExpiresAt;

    SFile(const std::uint32_t size, const TickType expiresAt);
    SFile(SFile&&) = default;
    SFile& operator=(SFile&&) = default;

    SFile(SFile const&) = delete;
    SFile& operator=(SFile const&) = delete;

	void Remove(const TickType now);
    auto RemoveExpiredReplicas(const TickType now) -> std::size_t;

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetSize() const -> std::uint32_t
    {return mSize;}
};

struct SReplica
{
private:
    SFile* mFile;
    CStorageElement* mStorageElement;
    std::uint32_t mCurSize = 0;

public:
    enum STATE
    {
        TRANSFERRING,
        COMPLETE,
        DELETED
    };
    STATE mState = TRANSFERRING;

public:
    std::size_t mIndexAtStorageElement;

    TickType mExpiresAt;

    SReplica(SFile* const file, CStorageElement* const storageElement, const std::size_t indexAtStorageElement);

    SReplica(SReplica&&) = default;
    SReplica& operator=(SReplica&&) = default;

    SReplica(SReplica const&) = delete;
    SReplica& operator=(SReplica const&) = delete;

    auto Increase(std::uint32_t amount, const TickType now) -> std::uint32_t;
	void OnRemoveByFile(const TickType now);
	void Remove(const TickType now);

	inline bool IsComplete() const
	{return mState == COMPLETE;}
	inline bool IsDeleted() const
	{return mState == DELETED;}

    inline auto GetFile() -> SFile*
    {return mFile;}
    inline auto GetFile() const -> const SFile*
    {return mFile;}
    inline auto GetStorageElement() -> CStorageElement*
    {return mStorageElement;}
    inline auto GetCurSize() const -> std::uint32_t
    {return mCurSize;}
};

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

class ISite
{
private:
	IdType mId;
    std::string mName;
	std::string mLocationName;

protected:
	std::unordered_map<IdType, std::size_t> mDstSiteIdToLinkSelectorIdx;

public:
    std::vector<std::unique_ptr<CLinkSelector>> mLinkSelectors;
	ISite(std::string&& name, std::string&& locationName);
	virtual ~ISite() = default;

	ISite(ISite&&) = default;
	ISite& operator=(ISite&&) = default;

	ISite(ISite const&) = delete;
	ISite& operator=(ISite const&) = delete;

	inline bool operator==(const ISite& b) const
	{return mId == b.mId;}
	inline bool operator!=(const ISite& b) const
	{return mId != b.mId;}

	virtual auto CreateLinkSelector(const ISite* const dstSite, const std::uint32_t bandwidth) -> CLinkSelector*;
    virtual auto CreateStorageElement(std::string&& name) -> CStorageElement* = 0;

	auto GetLinkSelector(const ISite* const dstSite) -> CLinkSelector*;

	inline auto GetId() const -> IdType
	{return mId;}
    inline auto GetName() const -> const std::string&
    {return mName;}
	inline auto GetLocationName() const -> const std::string&
	{return mLocationName;}
};

class CGridSite : public ISite
{
public:
	std::vector<std::unique_ptr<CStorageElement>> mStorageElements;

	CGridSite(std::string&& name, std::string&& locationName);
	CGridSite(CGridSite&&) = default;
	CGridSite& operator=(CGridSite&&) = default;

	CGridSite(CGridSite const&) = delete;
	CGridSite& operator=(CGridSite const&) = delete;

	auto CreateStorageElement(std::string&& name) -> CStorageElement*;
};

class CRucio
{
public:
    std::vector<std::unique_ptr<SFile>> mFiles;
    std::vector<std::unique_ptr<CGridSite>> mGridSites;

    CRucio();
    auto CreateFile(const std::uint32_t size, const TickType expiresAt) -> SFile*;
    auto CreateGridSite(std::string&& name, std::string&& locationName) -> CGridSite*;
    auto RunReaper(const TickType now) -> std::size_t;
};
