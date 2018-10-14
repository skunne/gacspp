#pragma once

#include "CCloudGCP.hpp"
#include "CRucio.hpp"

#include <cassert>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#define ONE_GiB (1073741824.0) // 2^30
#define BYTES_TO_GiB(x) ((x)/ONE_GiB)
#define SECONDS_PER_MONTH (2592000.0)
#define SECONDS_TO_MONTHS(x) ((x)/SECONDS_PER_MONTH)

namespace gcp
{

	CBucket::CBucket(std::string&& name, CRegion* region)
		: CStorageElement(std::move(name), region)
	{
		mBucketEvents.reserve(32768);
	}
	void CBucket::OnIncreaseReplica(std::uint64_t amount, std::uint64_t now)
	{
		mBucketEvents.emplace_back(now, amount);
		CStorageElement::OnIncreaseReplica(amount, now);
	}
	void CBucket::OnRemoveReplica(const SReplica& replica, std::uint64_t now)
	{
		mBucketEvents.emplace_back(now, -(static_cast<std::int64_t>(replica.GetCurSize())));
		CStorageElement::OnRemoveReplica(replica, now);
	}

	double CBucket::CalculateStorageCosts(std::uint64_t now)
	{
		const CRegion* const region = dynamic_cast<CRegion*>(mSite);
		assert(region);
		const double price = region->mStoragePriceCHF;
		double costs = 0;
		std::uint64_t timeOffset = mTimeAtLastReset;
		std::uint64_t usedStorageAtGivenTime = mStorageAtLastReset;
		mBucketEvents.emplace_back(now, 0);
		for (const auto &storageEvent : mBucketEvents)
		{
			assert(storageEvent.first >= timeOffset);
			const double timeDiff = storageEvent.first - timeOffset;
			if (timeDiff > 0)
			{
				const double storageGiB = BYTES_TO_GiB(usedStorageAtGivenTime);
				const double costsOfTimeDiff = storageGiB * price * SECONDS_TO_MONTHS(timeDiff);
				costs += costsOfTimeDiff;
				timeOffset = storageEvent.first;
			}
			usedStorageAtGivenTime += storageEvent.second;
		}
		assert(usedStorageAtGivenTime == mUsedStorage);
		mTimeAtLastReset = now;
		mStorageAtLastReset = mUsedStorage;
		mBucketEvents.clear();
		return costs;
	}


	CRegion::CRegion(std::uint32_t multiLocationIdx, std::string&& name, std::string&& locationName, double storagePriceCHF, std::string&& skuId)
		: ISite(std::move(name), std::move(locationName)),
		mStoragePriceCHF(storagePriceCHF),
		mSKUId(std::move(skuId)),
		mMultiLocationIdx(multiLocationIdx)
	{}
	CBucket &CRegion::CreateStorageElement(std::string&& name)
	{
		mStorageElements.emplace_back(std::move(name), this);
		return mStorageElements.back();
	}
	double CRegion::CalculateStorageCosts(std::uint64_t now)
	{
		double regionCosts = 0;
		for (auto &bucket : mStorageElements)
			regionCosts += bucket.CalculateStorageCosts(now);
		return regionCosts;
	}

	typedef std::vector<std::pair<std::uint64_t, double>> PriceInfoType;
	static double CalculateNetworkCosts(std::uint64_t traffic, PriceInfoType::const_iterator curLevelIt, const PriceInfoType::const_iterator &endIt, std::uint64_t prevThreshold = 0)
	{
		assert(curLevelIt->first >= prevThreshold);
		const std::uint64_t threshold = curLevelIt->first - prevThreshold;
		PriceInfoType::const_iterator nextLevelIt = curLevelIt + 1;
		if (traffic <= threshold || nextLevelIt == endIt)
			return BYTES_TO_GiB(traffic) * curLevelIt->second;
		const double lowerLevelCosts = CalculateNetworkCosts(traffic - threshold, nextLevelIt, endIt, curLevelIt->first);
		return (BYTES_TO_GiB(threshold) * curLevelIt->second) + lowerLevelCosts;
	}


	CRegion& CCloud::CreateRegion(std::uint32_t multiLocationIdx, std::string&& name, std::string&& locationName, double storagePriceCHF, std::string&& skuId)
	{
		mRegions.emplace_back(multiLocationIdx, std::move(name), std::move(locationName), storagePriceCHF, std::move(skuId));
		return mRegions.back();
	}

	bool CCloud::IsSameLocation(const CRegion& r1, const CRegion& r2) const
	{
		return r1.GetName() == r1.GetName();
	}
	bool CCloud::IsSameMultiLocation(const CRegion& r1, const CRegion& r2) const
	{
		if (r1.GetMultiLocationIdx() == r2.GetMultiLocationIdx())
			return !IsSameLocation(r1, r2);
		return false;
	}
	void CCloud::ProcessBilling(std::uint64_t now)
	{
		/*
		for transfer in self.transfer_list:
		transfer.update(current_time)
		*/
		double totalStorageCosts = 0;
		for (auto &region : mRegions)
		{
			const double regionCosts = region.CalculateStorageCosts(now);
			//storageCosts[bucket.GetName()] = bucketCosts;
			totalStorageCosts += regionCosts;
		}
		double totalNetworkCosts = 0;
		/*
		for (const auto &linkSelector : mLinkSelectors)
		{
			std::uint64_t traffic = 0;
			for (const auto &link : linkSelector.mLinks)
			{
				traffic += link.mUsedTraffic;
				link.mUsedTraffic = 0;
			}
			const double linkSelectorCosts = CalculateNetworkCosts(traffic, linkSelector.PriceInfo.cbegin(), linkSelector.PriceInfo.cend());
			totalNetworkCosts += linkSelectorCosts;
		}*/
	}
	void CCloud::SetupDefaultRegions()
	{
		assert(mRegions.empty());

		/*
		self.multi_locations['asia'] = ['asia', 'asia-northeast1', 'asia-south1', 'asia-east1', 'asia-southeast1']
		self.multi_locations['europe'] = ['europe', 'europe-west1', 'europe-west2', 'europe-west3', 'europe-west4']
		self.multi_locations['us'] = ['us', 'us-central1', 'us-west1', 'us-east1', 'us-east4', 'northamerica-northeast1']
		self.multi_locations['southamerica-east1'] = ['southamerica-east1']
		#self.multi_locations['northamerica-northeast1'] = ['northamerica-northeast1']
		self.multi_locations['australia-southeast1'] = ['australia-southeast1']
		*/
		/*
		AddMultiLocation("asia", 0);
		AddMultiLocation("australia-southeast1", 1);
		AddMultiLocation("europe", 2);
		AddMultiLocation("southamerica-east1", 3);
		AddMultiLocation("us", 4);
		*/
		// 0 = asia
		CreateRegion(0, "asia", "Asia", 0.02571790, "E653-0A40-3B69");
		CreateRegion(0, "asia-northeast1", "Tokyo", 0.02275045, "1845-1496-2891");
		CreateRegion(0, "asia-east1", "Taiwan", 0.01978300, "BAE2-255B-64A7");
		CreateRegion(0, "asia-southeast1", "Singapore", 0.01978300, "76BA-5CAD-4338");
		CreateRegion(0, "asia-south1", "Mumbai", 0.02275045, "2717-BEFE-3773");

		// 1 = australia-southeast1
		CreateRegion(1, "australia-southeast1", "Sydney", 0.02275045, "CF63-3CCD-F6EC");

		// 2 = europe
		CreateRegion(2, "europe", "Europe", 0.02571790, "EC40-8747-D6FF");
		CreateRegion(2, "europe-west1", "Belgium", 0.01978300, "A703-5CB6-E0BF");
		CreateRegion(2, "europe-west2", "London", 0.02275045, "BB55-3E5A-405C");
		CreateRegion(2, "europe-west3", "Frankfurt", 0.02275045, "F272-7933-F065");
		CreateRegion(2, "europe-west4", "Netherlands", 0.01978300, "89D8-0CF9-9F2E");

		// 3 = southamerica-east1
		CreateRegion(3, "southamerica-east1", "Sao Paulo", 0.03462025, "6B9B-6AB4-AC59");

		// 4 = us
		CreateRegion(4, "us", "US", 0.02571790, "0D5D-6E23-4250");
		CreateRegion(4, "us-central1", "Iowa", 0.01978300, "E5F0-6A5D-7BAD");
		CreateRegion(4, "us-west1", "Oregon", 0.01978300, "E5F0-6A5D-7BAD");
		CreateRegion(4, "us-east1", "South Carolina", 0.01978300, "E5F0-6A5D-7BAD");
		CreateRegion(4, "us-east4", "Northern Virginia", 0.02275045, "5F7A-5173-CF5B");

		//CreateRegion("us", "northamerica-northeast1", "Montreal", 0.02275045, "E466-8D73-08F4");
		//create_region("northamerica-northeast1', 'northamerica-northeast1', 'Montreal', 0.02275045, "E466-8D73-08F4")

	}

	void CCloud::SetupDefaultLinkSelectors()
	{
		assert(!mRegions.empty());
		for (const auto& srcRegion : mRegions)
		{
			for (const auto& dstRegion : mRegions)
			{
				//if (&srcRegion == &dstRegion)
				//	continue;
				//srcRegion.CreateLinkSelector(dstRegion);
			}
		}
	}

	void CCloud::SetupDefaultNetworkCosts()
	{
		assert(!mRegions.empty());

		/*
		eu - apac EF0A-B3BA-32CA 0.1121580 0.1121580 0.1028115 0.0747720
		na - apac 6B37-399C-BF69 0.0000000 0.1121580 0.1028115 0.0747720
		na - eu   C7FF-4F9E-C0DB 0.0000000 0.1121580 0.1028115 0.0747720

		au - apac CDD1-6B91-FDF8 0.1775835 0.1775835 0.1682370 0.1401975
		au - eu   1E7D-CBB0-AF0C 0.1775835 0.1775835 0.1682370 0.1401975
		au - na   27F0-D54C-619A 0.1775835 0.1775835 0.1682370 0.1401975
		au - sa   7F66-C883-4D7D 0.1121580 0.1121580 0.1028115 0.0747720
		apac - sa 1F9A-A9AC-FFC3 0.1121580 0.1121580 0.1028115 0.0747720
		eu - sa   96EB-C6ED-FBDE 0.1121580 0.1121580 0.1028115 0.0747720
		na - sa   BB86-91E8-5450 0.1121580 0.1121580 0.1028115 0.0747720
		*/
		//setup bucket to bucket transfer cost
		
		PriceInfoType priceSameRegion = { {0,0} };
		PriceInfoType priceSameMulti = { {1,0.0093465} };
		
		//cost_ww = {"asia": {}, "australia-southeast1": {}, "europe": {}, "southamerica-east1": {}, "us": {}}
		typedef std::unordered_map<std::uint32_t, PriceInfoType> InnerMapType;
		typedef std::unordered_map<std::uint32_t, InnerMapType> OuterMapType;
		/*
		std::unordered_map<std::string, std::unordered_map<std::string, PriceInfoType>> cost_ww  
		{ 
			{ "asia", {	{ "australia-southeast1", {{ 1024, 0.1775835 }, { 10240, 0.1682370 }, { 10240, 0.1401975 }}},
						{ "europe",{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
						{ "southamerica-east1",{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
						{ "us",{ {1, 0.0}, { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } }
					 }
			},
			{ "australia-southeast1", {	{ "europe",{ { 1024, 0.1775835 },{ 10240, 0.1682370 },{ 10240, 0.1401975 } } },
										{ "southamerica-east1",{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
										{ "us",{ { 1024, 0.1775835 },{ 10240, 0.1682370 },{ 10240, 0.1401975 } } }
									  }
			},
			{ "europe", {	{ "southamerica-east1",{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
							{ "us",{ { 1, 0.0 },{ 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } }
						}
			},
			{ "southamerica-east1", { { "us",{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } } } }
		};
		*/
		OuterMapType priceWW
		{ 
			{ 0, {	{ 1, {{ 1024, 0.1775835 }, { 10240, 0.1682370 }, { 10240, 0.1401975 }}},
					{ 2,{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
					{ 3,{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
					{ 4,{ {1, 0.0}, { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } }
				 }
			},
			{ 1, {	{ 2,{ { 1024, 0.1775835 },{ 10240, 0.1682370 },{ 10240, 0.1401975 } } },
					{ 3,{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
					{ 4,{ { 1024, 0.1775835 },{ 10240, 0.1682370 },{ 10240, 0.1401975 } } }
				 }
			},
			{ 2, {	{ 3,{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
					{ 4,{ { 1, 0.0 },{ 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } }
				 }
			},
			{ 3, { { 4,{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } } } }
		};
		for (auto& srcRegion : mRegions)
		{
			const auto srcRegionMultiLocationIdx = srcRegion.GetMultiLocationIdx();
			for (auto& dstRegion : mRegions)
			{
				const auto dstRegionMultiLocationIdx = dstRegion.GetMultiLocationIdx();
				CLinkSelector& linkSelector = srcRegion.CreateLinkSelector(dstRegion);
				const bool isSameLocation = IsSameLocation(srcRegion, dstRegion);
				const bool isSameMultiLocation = IsSameMultiLocation(srcRegion, dstRegion);
				if (!isSameLocation && !isSameMultiLocation)
				{
					OuterMapType::const_iterator outerIt = priceWW.find(srcRegionMultiLocationIdx);
					InnerMapType::const_iterator innerIt;
					if(outerIt == priceWW.cend())
					{
						outerIt = priceWW.find(dstRegionMultiLocationIdx);
						assert(outerIt != priceWW.cend());
						innerIt = outerIt->second.find(srcRegionMultiLocationIdx);
					}
					else
						innerIt = outerIt->second.find(dstRegionMultiLocationIdx);
					assert(innerIt != outerIt->second.cend());
					//linkselector.network_price_chf = innerIt->second;
				}
				else if (isSameLocation)
				{
					// 2. case: r1 and r2 are the same region
					//linkselector.network_price_chf = priceSameRegion
				}
				else
				{
					// 3. case: region r1 is inside the multi region r2
					//linkselector.network_price_chf = priceSameMulti
				}
			}
		}
		//download apac      1F8B-71B0-3D1B 0.0000000 0.1121580 0.1028115 0.0747720
		//download australia 9B2D-2B7D-FA5C 0.1775835 0.1775835 0.1682370 0.1401975
		//download china     4980-950B-BDA6 0.2149695 0.2149695 0.2056230 0.1869300
		//download us emea   22EB-AAE8-FBCD 0.0000000 0.1121580 0.1028115 0.0747720
	}

	void CCloud::SetupDefaultOperationCosts(){}

	void CCloud::SetupDefaultCloud()
	{
		SetupDefaultRegions();
		SetupDefaultLinkSelectors();
		SetupDefaultNetworkCosts();
		SetupDefaultOperationCosts();
	}
}