#pragma once

#include "IBaseCloud.hpp"
#include "ISite.hpp"

#include "CStorageElement.hpp"

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
        //~CRegion();

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
		using IBaseCloud::IBaseCloud;

		auto CreateRegion(std::uint32_t multiLocationIdx, std::string&& name, std::string&& locationName, double storagePriceCHF, std::string&& skuId) -> CRegion* final;
		auto ProcessBilling(TickType now) -> std::pair<double, std::pair<double, double>> final;
		void SetupDefaultCloud() final;

        bool TryConsumeConfig(const nlohmann::json& json) final;
	};
}
