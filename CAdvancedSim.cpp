#include <cassert>
#include <sstream>

#include "COutput.hpp"
#include "CRucio.hpp"
#include "CCloudGCP.hpp"
#include "CommonScheduleables.hpp"
#include "CAdvancedSim.hpp"

#include "sqlite3.h"


bool InsertSite(ISite* site);
bool InsertStorageElement(CStorageElement* storageElement);
bool InsertLinkSelector(CLinkSelector* linkselector);

void CAdvancedSim::SetupDefaults()
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

    CStorageElement::mOutputQueryIdx = COutput::GetRef().AddPreparedSQLStatement("INSERT INTO Replicas VALUES(?, ?);");


    ////////////////////////////
    // setup grid and clouds
    ////////////////////////////
    mRucio.reset(new CRucio);

    CGridSite* asgc = mRucio->CreateGridSite("ASGC", "asia");
    CGridSite* cern = mRucio->CreateGridSite("CERN", "europe");
    CGridSite* bnl = mRucio->CreateGridSite("BNL", "us");

    std::vector<CStorageElement*> gridStoragleElements {
                asgc->CreateStorageElement("TAIWAN_DATADISK"),
                cern->CreateStorageElement("CERN_DATADISK"),
                bnl->CreateStorageElement("BNL_DATADISK")
            };

    for(std::unique_ptr<IBaseCloud>& cloud : mClouds)
    {
        cloud->SetupDefaultCloud();
        for(std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
        {
            for(std::unique_ptr<ISite>& cloudSite : cloud->mRegions)
            {
                auto region = dynamic_cast<gcp::CRegion*>(cloudSite.get());
                gridSite->CreateLinkSelector(region, ONE_GiB / 32);
                region->CreateLinkSelector(gridSite.get(), ONE_GiB / 128);
            }
        }
        mSchedule.emplace(new CBillingGenerator(this));
    }


    ////////////////////////////
    // setup scheuleables
    ////////////////////////////
    std::shared_ptr<CDataGenerator> dataGen(new CDataGenerator(this, 75, 0));
    dataGen->mStorageElements = gridStoragleElements;

    std::shared_ptr<CReaper> reaper(new CReaper(mRucio.get(), 600, 600));

    std::shared_ptr<CTransferManager> x2cTransferMgr(new CTransferManager(20, 100));
    std::shared_ptr<CTransferGeneratorSrcPrio> x2cTransferGen(new CTransferGeneratorSrcPrio(this, x2cTransferMgr.get(), 25));
    x2cTransferGen->mTransferNumberGen->mSoftmaxScale = 10;
    x2cTransferGen->mTransferNumberGen->mSoftmaxOffset = 75;

    std::shared_ptr<CHeartbeat> heartbeat(new CHeartbeat(this, x2cTransferMgr, x2cTransferMgr, 10000, 10000));
    heartbeat->mProccessDurations["DataGen"] = &(dataGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["X2CTransferUpdate"] = &(x2cTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["X2CTransferGen"] = &(x2cTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["Reaper"] = &(reaper->mUpdateDurationSummed);


    for(std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
    {
        ok = InsertSite(gridSite.get());
        assert(ok);
        for(std::unique_ptr<CStorageElement>& storageElement : gridSite->mStorageElements)
        {
            ok = InsertStorageElement(storageElement.get());
            assert(ok);
            x2cTransferGen->mSrcStorageElementIdToPrio[storageElement->GetId()] = 0;
        }
        for(std::unique_ptr<CLinkSelector>& linkselector : gridSite->mLinkSelectors)
        {
            ok = InsertLinkSelector(linkselector.get());
            assert(ok);
        }
    }

    for(std::unique_ptr<ISite>& cloudSite : mClouds[0]->mRegions)
    {
        auto region = dynamic_cast<gcp::CRegion*>(cloudSite.get());
        assert(region);
        ok = InsertSite(region);
        assert(ok);
        for (std::unique_ptr<gcp::CBucket>& bucket : region->mStorageElements)
        {
            ok = InsertStorageElement(bucket.get());
            assert(ok);
            x2cTransferGen->mSrcStorageElementIdToPrio[bucket->GetId()] = 1;
            x2cTransferGen->mDstStorageElements.push_back(bucket.get());
        }
        for(std::unique_ptr<CLinkSelector>& linkselector : region->mLinkSelectors)
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
