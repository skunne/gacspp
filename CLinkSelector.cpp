#include <cassert>

#include "CLinkSelector.hpp"



CLinkSelector::CLinkSelector(const std::uint32_t bandwidth, const IdType srcSiteId, const IdType dstSiteId)
	: mId(GetNewId()),
      mSrcSiteId(srcSiteId),
      mDstSiteId(dstSiteId),
      mBandwidth(bandwidth)
{}
