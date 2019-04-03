#include <cassert>
#include <sstream>

#include "COutput.hpp"
#include "CRucio.hpp"
#include "CCloudGCP.hpp"
#include "CommonScheduleables.hpp"
#include "CSimpleSim.hpp"

#include "sqlite3.h"


bool InsertSite(ISite* site)
{
    std::stringstream row;
    row << site->GetId() << ",'"
        << site->GetName() << "','"
        << site->GetLocationName() << "'";
    return COutput::GetRef().InsertRow("Sites", row.str());
}
bool InsertStorageElement(CStorageElement* storageElement)
{
    std::stringstream row;
    row << storageElement->GetId() << ","
        << storageElement->GetSite()->GetId() << ",'"
        << storageElement->GetName() << "'";
    return COutput::GetRef().InsertRow("StorageElements", row.str());
}
bool InsertLinkSelector(CLinkSelector* linkselector)
{
    std::stringstream row;
    row << linkselector->GetId() << ","
        << linkselector->GetSrcSiteId() << ","
        << linkselector->GetDstSiteId();
    return COutput::GetRef().InsertRow("LinkSelectors", row.str());
}

void CSimpleSim::SetupDefaults()
{
    ////////////////////////////
    // init output db
    ////////////////////////////
    COutput& output = COutput::GetRef();

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
    //INSERT INTO Transfers VALUES(?, ?, ?, ?, ?, ?);

    CStorageElement::mOutputQueryIdx = COutput::GetRef().AddPreparedSQLStatement("INSERT INTO Replicas VALUES(?, ?);");


    ////////////////////////////
    // setup grid and clouds
    ////////////////////////////
    mRucio = std::make_unique<CRucio>();

    CGridSite* asgc = mRucio->CreateGridSite("ASGC", "asia");
    CGridSite* cern = mRucio->CreateGridSite("CERN", "europe");
    CGridSite* bnl = mRucio->CreateGridSite("BNL", "us");

    std::vector<CStorageElement*> gridStoragleElements {
                asgc->CreateStorageElement("TAIWAN_DATADISK"),
                cern->CreateStorageElement("CERN_DATADISK"),
                bnl->CreateStorageElement("BNL_DATADISK")
            };

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
    dataGen->mStorageElements = gridStoragleElements;

    auto reaper = std::make_shared<CReaper>(mRucio.get(), 600, 600);

    auto g2cTransferMgr = std::make_shared<CTransferManager>(20, 100);
    auto g2cTransferNumGen = std::make_shared<CWavedTransferNumGen>(15, 500, 25, 0.075);
    auto g2cTransferGen = std::make_shared<CExponentialTransferGen>(this, g2cTransferMgr, g2cTransferNumGen, 25);
    g2cTransferGen->mSrcStorageElements = gridStoragleElements;

    auto c2cTransferMgr = std::make_shared<CTransferManager>(20, 100);
    auto c2cTransferNumGen = std::make_shared<CWavedTransferNumGen>(10, 40, 25, 0.075);
    auto c2cTransferGen = std::make_shared<CExponentialTransferGen>(this, c2cTransferMgr, c2cTransferNumGen, 25);

    auto heartbeat = std::make_shared<CHeartbeat>(this, g2cTransferMgr, c2cTransferMgr, SECONDS_PER_DAY, SECONDS_PER_DAY);
    heartbeat->mProccessDurations["DataGen"] = &(dataGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["G2CTransferUpdate"] = &(g2cTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["G2CTransferGen"] = &(g2cTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["C2CTransferUpdate"] = &(c2cTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["C2CTransferGen"] = &(c2cTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["Reaper"] = &(reaper->mUpdateDurationSummed);


    for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
    {
        ok = InsertSite(gridSite.get());
        assert(ok);
        for(const std::unique_ptr<CStorageElement>& storageElement : gridSite->mStorageElements)
        {
            ok = InsertStorageElement(storageElement.get());
            assert(ok);
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
            g2cTransferGen->mDstStorageElements.push_back(bucket.get());
            c2cTransferGen->mSrcStorageElements.push_back(bucket.get());
            c2cTransferGen->mDstStorageElements.push_back(bucket.get());
        }
        for(const std::unique_ptr<CLinkSelector>& linkselector : region->mLinkSelectors)
        {
            ok = InsertLinkSelector(linkselector.get());
            assert(ok);
        }
    }

    mSchedule.push(dataGen);
    mSchedule.push(reaper);
    mSchedule.push(g2cTransferMgr);
    mSchedule.push(g2cTransferGen);
    mSchedule.push(c2cTransferMgr);
    mSchedule.push(c2cTransferGen);
    mSchedule.push(heartbeat);
}
