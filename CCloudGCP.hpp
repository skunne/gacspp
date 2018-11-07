#pragma once

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

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
	virtual auto ProcessBilling(std::uint64_t now) -> std::pair<double, std::pair<double, double>> = 0;
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
		std::uint64_t mTimeAtLastReset = 0;
		std::uint64_t mStorageAtLastReset = 0;
		std::vector<std::pair<std::uint64_t, std::int64_t>> mBucketEvents;

	public:
		std::uint32_t mLogId;
		std::ofstream *mStorageLog;

		CBucket(std::string&& name, CRegion* region);
		CBucket(CBucket&&) = default;
		virtual void OnIncreaseReplica(std::uint64_t amount, std::uint64_t now) final;
		virtual void OnRemoveReplica(const SReplica* replica, std::uint64_t now) final;

		double CalculateStorageCosts(std::uint64_t now);
	};

	class CRegion : public ISite
	{
	private:
		std::string mSKUId;
		std::uint32_t mMultiLocationIdx;
		double mStoragePriceCHF;

	public:
        std::ofstream *mNetworkLog;
		std::vector<std::unique_ptr<CBucket>> mStorageElements;

		CRegion(std::uint32_t multiLocationIdx, std::string&& name, std::string&& locationName, double storagePriceCHF, std::string&& skuId);

		auto CreateStorageElement(std::string&& name) -> CBucket* final;
		double CalculateStorageCosts(std::uint64_t now);
		double CalculateNetworkCosts(std::uint64_t now, double& sumUsedTraffic);

		inline auto GetMultiLocationIdx() const -> std::uint32_t
		{return mMultiLocationIdx;}
		inline auto GetStoragePrice() const -> double
		{return mStoragePriceCHF;}
	};

	class CCloud final : public IBaseCloud
	{
	private:
		std::ofstream mStorageLog;
        std::ofstream mNetworkLog;

	public:
		//std::vector<std::unique_ptr<CRegion>> mRegions;

		CCloud(std::string&& name)
			: IBaseCloud(std::move(name))
		{}

		auto CreateRegion(std::uint32_t multiLocationIdx, std::string&& name, std::string&& locationName, double storagePriceCHF, std::string&& skuId) -> CRegion* final;
		auto ProcessBilling(std::uint64_t now) -> std::pair<double, std::pair<double, double>> final;
		void SetupDefaultCloud() final;
	};
}
