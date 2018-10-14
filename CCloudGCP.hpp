#pragma once

#include "CRucio.hpp"

#include <cassert>
#include <cstdint>
#include <string>
#include <vector>

#define ONE_GiB (1073741824.0) // 2^30
#define BYTES_TO_GiB(x) ((x)/ONE_GiB)
#define SECONDS_PER_MONTH (2592000.0)
#define SECONDS_TO_MONTHS(x) ((x)/SECONDS_PER_MONTH)

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
		CBucket(std::string&& name, CRegion* region);
		CBucket(CBucket&&) = default;
		virtual void OnIncreaseReplica(std::uint64_t amount, std::uint64_t now) override;
		virtual void OnRemoveReplica(const SReplica& replica, std::uint64_t now) override;
	
		double CalculateStorageCosts(std::uint64_t now);
	};

	class CRegion : public ISite
	{
	private:
		std::vector<CBucket> mStorageElements;
		std::string mSKUId;
		std::uint32_t mMultiLocationIdx;

	public:
		double mStoragePriceCHF;

		CRegion(std::uint32_t multiLocationIdx, std::string&& name, std::string&& locationName, double storagePriceCHF, std::string&& skuId);

		CBucket &CreateStorageElement(std::string&& name);
		double CalculateStorageCosts(std::uint64_t now);

		inline auto GetMultiLocationIdx() const -> std::uint32_t
		{return mMultiLocationIdx;}
		inline auto GetStorageElements() const -> const std::vector<CBucket>&
		{return mStorageElements;}
	};

	class CCloud
	{
	private:
		std::vector<CRegion> mRegions;

		void SetupDefaultRegions();
		void SetupDefaultLinkSelectors();
		void SetupDefaultNetworkCosts();
		void SetupDefaultOperationCosts();

		CRegion& CreateRegion(std::uint32_t multiLocationIdx, std::string&& name, std::string&& locationName, double storagePriceCHF, std::string&& skuId);
	public:
		
		bool IsSameLocation(const CRegion& r1, const CRegion& r2) const;
		bool IsSameMultiLocation(const CRegion& r1, const CRegion& r2) const;
		void ProcessBilling(std::uint64_t now);
		void SetupDefaultCloud();
		/*
		def __init__(self):
			self.region_list = []
			self.region_by_name = {}

			self.bucket_list = []
			self.bucket_by_name  = {}

			self.linkselector_list = []

			self.transfer_list = []

			self.multi_locations = {}

		def get_as_graph(self):
			graph = {}
			for src_bucket in self.bucket_list:
				src_name = src_bucket.name
				src_region = src_bucket.site_obj
				graph[src_name] = {}
				for dst_bucket in self.bucket_list:
					dst_name = dst_bucket.name
					dst_region = dst_bucket.site_obj
					w = 0
					ls = src_region.linkselector_by_name.get(dst_region.name)
					if ls != None:
						w = ls.get_weight()
					graph[src_name][dst_name] = w
			return graph

		def create_bucket(self, region, bucket_name, storage_type):
			assert bucket_name not in self.bucket_by_name, bucket_name

			region_obj = self.get_region_obj(region)
			if storage_type == Bucket.TYPE_MULTI:
				region_name = region_obj.name
				if region_name not in ['asia', 'europe', 'us']: # TODO needs better solution!
					raise RuntimeError('create_bucket: cannot create multi regional bucket in region {}'.format(region_name))

			new_bucket = region_obj.create_rse(bucket_name, storage_type)

			self.bucket_list.append(new_bucket)
			self.bucket_by_name[bucket_name] = new_bucket
			return new_bucket
		*/
	};
}