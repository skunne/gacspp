#include <cassert>

#include "ISite.hpp"

#include "COutput.hpp"
#include "CLinkSelector.hpp"
#include "CStorageElement.hpp"



ISite::ISite(const std::uint32_t multiLocationIdx, std::string&& name, const std::string& locationName)
	: mId(GetNewId()),
      mMultiLocationIdx(multiLocationIdx),
      mName(std::move(name))
{
    std::string row = std::to_string(mId) + ",'" + mName + "','" + locationName + "'";
    bool ok = COutput::GetRef().InsertRow("Sites", row);
    assert(ok);
}

ISite::~ISite() = default;

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
