#pragma once

#include <utility>
#include <vector>

#include "constants.h"

class ISite;

class CLinkSelector
{
private:
    //monitoring
    IdType mId;
    ISite* mSrcSite;
    ISite* mDstSite;

public:
    std::uint32_t mDoneTransfers = 0;
    std::uint32_t mFailedTransfers = 0;

public:
	typedef std::vector<std::pair<std::uint64_t, double>> PriceInfoType;

	CLinkSelector(const std::uint32_t bandwidth, ISite* srcSite, ISite* dstSite);

    inline auto GetId() const -> IdType
    {return mId;}
    auto GetSrcSite() const -> ISite*;
    auto GetDstSite() const -> ISite*;
    auto GetSrcSiteId() const -> IdType;
    auto GetDstSiteId() const -> IdType;

    inline auto GetWeight() const -> double
    { return mNetworkPrice.back().second; }

    PriceInfoType mNetworkPrice = {{0,0}};
    std::uint64_t mUsedTraffic = 0;
    std::uint32_t mNumActiveTransfers = 0;
    std::uint32_t mBandwidth;
};
