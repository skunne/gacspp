#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_set>

class CStorageElement;
class CLinkSelector
{};

struct SFile
{
public:
    typedef std::uint64_t IdType;

private:
    static IdType IdCounter;

    IdType mId;
    std::uint32_t mSize;

public:
    std::uint64_t mExpiresAt;

    SFile(std::uint32_t size, std::uint64_t expiresAt);
    SFile(SFile&&) = default;
    SFile& operator=(SFile&&) = default;

    SFile(SFile const&) = delete;
    SFile& operator=(SFile const&) = delete;

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetSize() const -> std::uint32_t
    {return mSize;}
};

struct SReplica
{
private:
    SFile *mFile;
    CStorageElement *mStorageElement;
    std::uint32_t mCurSize = 0;

public:
    std::size_t mIndexAtStorageElement;

    SReplica(SFile* file, CStorageElement* storageElement, std::size_t indexAtStorageElement);
    SReplica(SReplica&&) = default;
    SReplica& operator=(SReplica&&) = default;

    SReplica(SReplica const&) = delete;
    SReplica& operator=(SReplica const&) = delete;

    auto Increase(std::uint32_t amount, std::uint64_t now) -> std::uint32_t;
    void Remove(std::uint64_t now);

    inline auto GetFile() const -> SFile*
    {return mFile;}
    inline auto GetStorageElement() const -> CStorageElement*
    {return mStorageElement;}
    inline auto GetCurSize() const -> std::uint32_t
    {return mCurSize;}
};

class ISite
{
private:
	std::vector<CLinkSelector> mLinkSelectors;
    std::string mName;
	std::string mLocationName;

public:
	ISite(std::string&& name, std::string&& locationName);
	virtual ~ISite() = default;

	ISite(ISite&&) = default;
	ISite& operator=(ISite&&) = default;

	ISite(ISite const&) = delete;
	ISite& operator=(ISite const&) = delete;

	virtual auto CreateLinkSelector(ISite& dstSite) -> CLinkSelector& { mLinkSelectors.emplace_back(); return mLinkSelectors.back(); };
    virtual auto CreateStorageElement(std::string&& name) -> CStorageElement& = 0;
    inline auto GetName() const -> const std::string&
    {return mName;}
	inline auto GetLocationName() const -> const std::string&
	{return mLocationName;}
};
class CGridSite : public ISite
{
private:
	std::vector<CStorageElement> mStorageElements;

public:
	CGridSite(std::string&& name, std::string&& locationName);
	CGridSite(CGridSite&&) = default;
	CGridSite& operator=(CGridSite&&) = default;

	CGridSite(CGridSite const&) = delete;
	CGridSite& operator=(CGridSite const&) = delete;

	virtual auto CreateStorageElement(std::string&& name) -> CStorageElement&;
};

class CStorageElement
{
private:
    std::unordered_set<SFile::IdType> mFileIds;
    std::vector<SReplica> mReplicas;
    std::string mName;

protected:
    ISite* mSite;
    std::uint64_t mUsedStorage = 0;

public:
	CStorageElement(std::string&& name, ISite* site);
    CStorageElement(CStorageElement&&) = default;
    CStorageElement& operator=(CStorageElement&&) = default;

    CStorageElement(CStorageElement const&) = delete;
    CStorageElement& operator=(CStorageElement const&) = delete;

    auto CreateReplica(SFile& file) -> SReplica&;
    virtual void OnIncreaseReplica(std::uint64_t amount, std::uint64_t now);
    virtual void OnRemoveReplica(const SReplica& replica, std::uint64_t now);
    inline auto GetName() const -> const std::string&
    {return mName;}
    inline auto GetSite() const -> const ISite*
    {return mSite;}
};

class CRucio
{
public:
    std::vector<SFile> mFiles;

    CRucio();
    auto CreateFile(std::uint32_t size, std::uint64_t expiresAt) -> SFile&;
    auto RunReaper(std::uint64_t now) -> std::size_t;
};
