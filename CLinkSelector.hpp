#pragma once

#include <utility>
#include <vector>

#include "constants.h"



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

	CLinkSelector(const std::uint32_t bandwidth, const IdType srcSiteId, const IdType dstSiteId);

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
