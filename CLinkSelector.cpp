#include <cassert>

#include "CLinkSelector.hpp"
#include "COutput.hpp"



CLinkSelector::CLinkSelector(const std::uint32_t bandwidth, const IdType srcSiteId, const IdType dstSiteId)
	: mId(GetNewId()),
      mSrcSiteId(srcSiteId),
      mDstSiteId(dstSiteId),
      mBandwidth(bandwidth)
{
    std::string row = std::to_string(mId) + "," + std::to_string(mSrcSiteId) + "," + std::to_string(mDstSiteId);
    bool ok = COutput::GetRef().InsertRow("LinkSelectors", row);
    assert(ok);
}
