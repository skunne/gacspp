#pragma once

#include <string>
#include <memory>
#include <vector>

#include "constants.h"

#include "IConfigConsumer.hpp"
#include "ISite.hpp"

struct SFile;



class CGridSite : public ISite
{
public:
	std::vector<std::unique_ptr<CStorageElement>> mStorageElements;

	CGridSite(const std::uint32_t multiLocationIdx, std::string&& name, std::string&& locationName);
	CGridSite(CGridSite&&) = default;
	CGridSite& operator=(CGridSite&&) = default;

	CGridSite(CGridSite const&) = delete;
	CGridSite& operator=(CGridSite const&) = delete;

	auto CreateStorageElement(std::string&& name) -> CStorageElement*;
};

class CRucio : public IConfigConsumer
{
public:
    std::vector<std::unique_ptr<SFile>> mFiles;
    std::vector<std::unique_ptr<CGridSite>> mGridSites;

    CRucio();
    ~CRucio();

    auto CreateFile(const std::uint32_t size, const TickType expiresAt) -> SFile*;
    auto CreateGridSite(const std::uint32_t multiLocationIdx, std::string&& name, std::string&& locationName) -> CGridSite*;
    auto RunReaper(const TickType now) -> std::size_t;

    bool TryConsumeConfig(const nlohmann::json& json) final;
};
