//#include <algorithm>
#include <iostream>

#include "json.hpp"

#include "CLinkSelector.hpp"
#include "CRucio.hpp"
#include "CStorageElement.hpp"
#include "SFile.hpp"



CGridSite::CGridSite(const std::uint32_t multiLocationIdx, std::string&& name, const std::string& locationName)
	: ISite(multiLocationIdx, std::move(name), locationName)
{
	mStorageElements.reserve(8);
}

auto CGridSite::CreateStorageElement(std::string&& name) -> CStorageElement*
{
    CStorageElement* newStorageElement = new CStorageElement(std::move(name), this);
    mStorageElements.emplace_back(newStorageElement);
    return newStorageElement;
}



CRucio::CRucio()
{
    mFiles.reserve(150000);
}

CRucio::~CRucio() = default;

auto CRucio::CreateFile(const std::uint32_t size, const TickType expiresAt) -> SFile*
{
    SFile* newFile = new SFile(size, expiresAt);
    mFiles.emplace_back(newFile);
    return newFile;
}
auto CRucio::CreateGridSite(const std::uint32_t multiLocationIdx, std::string&& name, const std::string& locationName) -> CGridSite*
{
    CGridSite* newSite = new CGridSite(multiLocationIdx, std::move(name), locationName);
    mGridSites.emplace_back(newSite);
    return newSite;
}

auto CRucio::RunReaper(const TickType now) -> std::size_t
{
    const std::size_t numFiles = mFiles.size();

    if(numFiles == 0)
        return 0;

    std::size_t frontIdx = 0;
    std::size_t backIdx = numFiles - 1;

    while(backIdx > frontIdx && mFiles[backIdx]->mExpiresAt <= now)
    {
        mFiles[backIdx]->Remove(now);
        mFiles.pop_back();
        --backIdx;
    }

    for(;frontIdx < backIdx; ++frontIdx)
    {
        std::unique_ptr<SFile>& curFile = mFiles[frontIdx];
        if(curFile->mExpiresAt <= now)
        {
            std::swap(curFile, mFiles[backIdx]);
            do
            {
                mFiles[backIdx]->Remove(now);
                mFiles.pop_back();
                --backIdx;
            } while(backIdx > frontIdx && mFiles[backIdx]->mExpiresAt <= now);
        }
        else
            curFile->RemoveExpiredReplicas(now);
    }

    if(backIdx == 0 && mFiles.back()->mExpiresAt <= now)
    {
        mFiles[backIdx]->Remove(now);
        mFiles.pop_back();
    }
    return numFiles - mFiles.size();
}

bool CRucio::TryConsumeConfig(const json& json)
{
    json::const_iterator rootIt = json.find("rucio");
    if( rootIt == json.cend() )
        return false;

    for( const auto& [key, value] : rootIt.value().items() )
    {
        if( key == "sites" )
        {
            for(const auto& siteJson : value)
            {
                std::unique_ptr<std::uint32_t> multiLocationIdx;
                std::string siteName, siteLocation;
                nlohmann::json storageElementsJson;
                for(const auto& [siteJsonKey, siteJsonValue] : siteJson.items())
                {
                    if(siteJsonKey == "multiLocationIdx")
                        multiLocationIdx = std::make_unique<std::uint32_t>(siteJsonValue.get<std::uint32_t>());
                    else if(siteJsonKey == "name")
                        siteName = siteJsonValue.get<std::string>();
                    else if(siteJsonKey == "location")
                        siteLocation = siteJsonValue.get<std::string>();
                    else if(siteJsonKey == "storageElements")
                        storageElementsJson = siteJsonValue;
                    else
                        std::cout << "Ignoring unknown attribute while loading sites: " << siteJsonKey << std::endl;
                }

                if(multiLocationIdx == nullptr)
                {
                    std::cout << "Couldn't find multiLocationIdx attribute of site" << std::endl;
                    continue;
                }

                if (siteName.empty())
                {
                    std::cout << "Couldn't find name attribute of site" << std::endl;
                    continue;
                }

                if (siteLocation.empty())
                {
                    std::cout << "Couldn't find location attribute of site: " << siteName << std::endl;
                    continue;
                }

                std::cout << "Adding site " << siteName << " in " << siteLocation << std::endl;
                CGridSite *site = CreateGridSite(*multiLocationIdx, std::move(siteName), siteLocation);

                if (storageElementsJson.empty())
                {
                    std::cout << "No storage elements to create for this site" << std::endl;
                    continue;
                }

                for(const auto& storageElementJson : storageElementsJson)
                {
                    std::string storageElementName;
                    for(const auto& [storageElementJsonKey, storageElementJsonValue] : storageElementJson.items())
                    {
                        if(storageElementJsonKey == "name")
                            storageElementName = storageElementJsonValue.get<std::string>();
                        else
                            std::cout << "Ignoring unknown attribute while loading StorageElements: " << storageElementJsonKey << std::endl;
                    }

                    if (storageElementName.empty())
                    {
                        std::cout << "Couldn't find name attribute of StorageElement" << std::endl;
                        continue;
                    }

                    std::cout << "Adding StorageElement " << storageElementName << std::endl;
                    site->CreateStorageElement(std::move(storageElementName));
                }
            }
        }
    }
    return true;
}
