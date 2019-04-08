#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "constants.h"

class CLinkSelector;
class CStorageElement;



// 0 = asia
// 1 = australia-southeast1
// 2 = europe
// 3 = southamerica-east1
// 4 = us

class ISite
{
private:
	IdType mId;
    std::uint32_t mMultiLocationIdx;
    std::string mName;

protected:
	std::unordered_map<IdType, std::size_t> mDstSiteIdToLinkSelectorIdx;

public:
    std::vector<std::unique_ptr<CLinkSelector>> mLinkSelectors;
	ISite(const std::uint32_t multiLocationIdx, std::string&& name, const std::string& locationName);
	virtual ~ISite();

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
	inline auto GetMultiLocationIdx() const -> std::uint32_t
	{return mMultiLocationIdx;}
    inline auto GetName() const -> const std::string&
    {return mName;}
};
