#include <cassert>
#include <sstream>

#include "sqlite3.h"

#include "CAdvancedSim.hpp"
#include "CCloudGCP.hpp"
#include "CConfigLoader.hpp"
#include "CRucio.hpp"
#include "COutput.hpp"
#include "CommonScheduleables.hpp"



bool InsertSite(ISite* site);
bool InsertStorageElement(CStorageElement* storageElement);
bool InsertLinkSelector(CLinkSelector* linkselector);


void CAdvancedSim::SetupDefaults()
{
    COutput& output = COutput::GetRef();
    CConfigLoader& config = CConfigLoader::GetRef();
    ////////////////////////////
    // init output db
    ////////////////////////////
    std::stringstream columns;
    bool ok = false;

    ok = output.CreateTable("Sites", "id BIGINT PRIMARY KEY, name varchar(64), locationName varchar(64)");
    assert(ok);

    ok = output.CreateTable("StorageElements", "id BIGINT PRIMARY KEY, siteId BIGINT, name varchar(64), FOREIGN KEY(siteId) REFERENCES Sites(id)");
    assert(ok);

    ok = output.CreateTable("LinkSelectors", "id BIGINT PRIMARY KEY, srcSiteId BIGINT, dstSiteId BIGINT, FOREIGN KEY(srcSiteId) REFERENCES Sites(id), FOREIGN KEY(dstSiteId) REFERENCES Sites(id)");
    assert(ok);

    ok = output.CreateTable("Files", "id BIGINT PRIMARY KEY, createdAt BIGINT, lifetime BIGINT, filesize INTEGER");
    assert(ok);

    ok = output.CreateTable("Replicas", "fileId BIGINT, storageElementId BIGINT, PRIMARY KEY(fileId, storageElementId)");
    assert(ok);

    columns << "id BIGINT PRIMARY KEY,"
            << "fileId BIGINT,"
            << "srcStorageElementId BIGINT,"
            << "dstStorageElementId BIGINT,"
            << "startTick BIGINT,"
            << "endTick BIGINT,"
            << "FOREIGN KEY(fileId) REFERENCES Files(id),"
            << "FOREIGN KEY(srcStorageElementId) REFERENCES StorageElements(id),"
            << "FOREIGN KEY(dstStorageElementId) REFERENCES StorageElements(id)";
    ok = output.CreateTable("Transfers", columns.str());
    assert(ok);

    CStorageElement::mOutputQueryIdx = output.AddPreparedSQLStatement("INSERT INTO Replicas VALUES(?, ?);");


    ////////////////////////////
    // setup grid and clouds
    ////////////////////////////
    mRucio = std::make_unique<CRucio>();
    mClouds.emplace_back(std::make_unique<gcp::CCloud>("GCP"));

    config.mConfigConsumer.push_back(mRucio.get());
    for(std::unique_ptr<IBaseCloud>& cloud : mClouds)
        config.mConfigConsumer.push_back(cloud.get());

    config.TryLoadConfig(std::filesystem::current_path() / "config" / "default.json");

    for(const std::unique_ptr<IBaseCloud>& cloud : mClouds)
    {
        cloud->SetupDefaultCloud();
        for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
        {
            for(const std::unique_ptr<ISite>& cloudSite : cloud->mRegions)
            {
                auto region = dynamic_cast<gcp::CRegion*>(cloudSite.get());
                gridSite->CreateLinkSelector(region, ONE_GiB / 32);
                region->CreateLinkSelector(gridSite.get(), ONE_GiB / 128);
            }
        }
        mSchedule.push(std::make_shared<CBillingGenerator>(this));
    }


    ////////////////////////////
    // setup scheuleables
    ////////////////////////////
    auto dataGen = std::make_shared<CDataGenerator>(this, 50, 0);
    for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
        for(const std::unique_ptr<CStorageElement>& gridStoragleElement : gridSite->mStorageElements)
            dataGen->mStorageElements.push_back(gridStoragleElement.get());

    auto reaper = std::make_shared<CReaper>(mRucio.get(), 600, 600);

    auto x2cTransferMgr = std::make_shared<CTransferManager>(20, 100);
    auto x2cTransferNumGen = std::make_shared<CWavedTransferNumGen>(12, 200, 25, 0.075);
    auto x2cTransferGen = std::make_shared<CSrcPrioTransferGen>(this, x2cTransferMgr, x2cTransferNumGen, 25);

    auto heartbeat = std::make_shared<CHeartbeat>(this, x2cTransferMgr, x2cTransferMgr, SECONDS_PER_DAY, SECONDS_PER_DAY);
    heartbeat->mProccessDurations["DataGen"] = &(dataGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["X2CTransferUpdate"] = &(x2cTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["X2CTransferGen"] = &(x2cTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["Reaper"] = &(reaper->mUpdateDurationSummed);


    for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
    {
        ok = InsertSite(gridSite.get());
        assert(ok);
        for(const std::unique_ptr<CStorageElement>& storageElement : gridSite->mStorageElements)
        {
            ok = InsertStorageElement(storageElement.get());
            assert(ok);
            x2cTransferGen->mSrcStorageElementIdToPrio[storageElement->GetId()] = 0;
        }
        for(const std::unique_ptr<CLinkSelector>& linkselector : gridSite->mLinkSelectors)
        {
            ok = InsertLinkSelector(linkselector.get());
            assert(ok);
        }
    }

    for(const std::unique_ptr<ISite>& cloudSite : mClouds[0]->mRegions)
    {
        auto region = dynamic_cast<gcp::CRegion*>(cloudSite.get());
        assert(region);
        ok = InsertSite(region);
        assert(ok);
        for (const std::unique_ptr<gcp::CBucket>& bucket : region->mStorageElements)
        {
            ok = InsertStorageElement(bucket.get());
            assert(ok);
            x2cTransferGen->mSrcStorageElementIdToPrio[bucket->GetId()] = 1;
            x2cTransferGen->mDstStorageElements.push_back(bucket.get());
        }
        for(const std::unique_ptr<CLinkSelector>& linkselector : region->mLinkSelectors)
        {
            ok = InsertLinkSelector(linkselector.get());
            assert(ok);
        }
    }

    mSchedule.push(dataGen);
    mSchedule.push(reaper);
    mSchedule.push(x2cTransferMgr);
    mSchedule.push(x2cTransferGen);
    mSchedule.push(heartbeat);
}
