#pragma once

#include <memory>
#include <string>
#include <vector>

#include "constants.h"
#include "CRucio.hpp"

class IBaseCloud
{
private:
	std::string mName;

public:
	std::vector<std::unique_ptr<ISite>> mRegions;

	IBaseCloud(std::string&& name)
		: mName(std::move(name))
	{}

	virtual auto CreateRegion(std::uint32_t multiLocationIdx, std::string&& name, std::string&& locationName, double storagePriceCHF, std::string&& skuId) -> ISite* = 0;
	virtual auto ProcessBilling(TickType now) -> std::pair<double, std::pair<double, double>> = 0;
	virtual void SetupDefaultCloud() = 0;

	inline auto GetName() const -> const std::string&
	{return mName;}
};

namespace gcp
{
	class CRegion;
	class CBucket : public CStorageElement
	{
	private:
		TickType mTimeAtLastReset = 0;
		TickType mStorageAtLastReset = 0;
		std::vector<std::pair<TickType, std::int64_t>> mBucketEvents;

	public:

		CBucket(std::string&& name, CRegion* region);
		CBucket(CBucket&&) = default;
		virtual void OnIncreaseReplica(std::uint64_t amount, TickType now) final;
		virtual void OnRemoveReplica(const SReplica* replica, TickType now) final;

		double CalculateStorageCosts(TickType now);
	};

	class CRegion : public ISite
	{
	private:
		std::string mSKUId;
		std::uint32_t mMultiLocationIdx;
		double mStoragePriceCHF;

	public:
		std::vector<std::unique_ptr<CBucket>> mStorageElements;

		CRegion(std::uint32_t multiLocationIdx, std::string&& name, std::string&& locationName, double storagePriceCHF, std::string&& skuId);

		auto CreateStorageElement(std::string&& name) -> CBucket* final;
		double CalculateStorageCosts(TickType now);
		double CalculateNetworkCosts(double& sumUsedTraffic, std::uint64_t& sumDoneTransfers);

		inline auto GetMultiLocationIdx() const -> std::uint32_t
		{return mMultiLocationIdx;}
		inline auto GetStoragePrice() const -> double
		{return mStoragePriceCHF;}
	};

	class CCloud final : public IBaseCloud
	{
	public:
		//std::vector<std::unique_ptr<CRegion>> mRegions;

		CCloud(std::string&& name)
			: IBaseCloud(std::move(name))
		{}

		auto CreateRegion(std::uint32_t multiLocationIdx, std::string&& name, std::string&& locationName, double storagePriceCHF, std::string&& skuId) -> CRegion* final;
		auto ProcessBilling(TickType now) -> std::pair<double, std::pair<double, double>> final;
		void SetupDefaultCloud() final;
	};
}
