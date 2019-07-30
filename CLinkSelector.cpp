#include <cassert>

#include "ISite.hpp"
#include "CLinkSelector.hpp"



CLinkSelector::CLinkSelector(const std::uint32_t bandwidth, ISite* srcSite, ISite* dstSite)
	: mId(GetNewId()),
      mSrcSite(srcSite),
      mDstSite(dstSite),
      mBandwidth(bandwidth)
{}

auto CLinkSelector::GetSrcSite() const -> ISite*
{return mSrcSite;}
auto CLinkSelector::GetDstSite() const -> ISite*
{return mDstSite;}
auto CLinkSelector::GetSrcSiteId() const -> IdType
{return mSrcSite->GetId();}
auto CLinkSelector::GetDstSiteId() const -> IdType
{return mDstSite->GetId();}
