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
    mRucio.reset(new CRucio);

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

    //proccesses
    std::vector<std::shared_ptr<CBillingGenerator>> billingGenerators;
    std::shared_ptr<CDataGenerator> dataGen(new CDataGenerator(this, 50, 0));
    std::shared_ptr<CReaper> reaper(new CReaper(mRucio.get(), 600, 600));


    std::shared_ptr<CTransferManager> g2cTransferMgr(new CTransferManager(20, 100));
    //std::shared_ptr<CTransferGeneratorUniform> g2cTransferGen(new CTransferGeneratorUniform(this, g2cTransferMgr.get(), 25));
    std::shared_ptr<CTransferGeneratorExponential> g2cTransferGen(new CTransferGeneratorExponential(this, g2cTransferMgr.get(), 25));
    g2cTransferGen->mTransferNumberGen->mSoftmaxScale = 15;
    g2cTransferGen->mTransferNumberGen->mSoftmaxOffset = 500;
    g2cTransferGen->mOverrideExpiration = true;

    std::shared_ptr<CTransferManager> c2cTransferMgr(new CTransferManager(20, 100));
    std::shared_ptr<CTransferGeneratorExponential> c2cTransferGen(new CTransferGeneratorExponential(this, c2cTransferMgr.get(), 25));
    c2cTransferGen->mTransferNumberGen->mSoftmaxScale = 10;
    c2cTransferGen->mTransferNumberGen->mSoftmaxOffset = 40;

    std::shared_ptr<CHearbeat> heartbeat(new CHearbeat(this, g2cTransferMgr, c2cTransferMgr, 10000, 10000));
    heartbeat->mProccessDurations["DataGen"] = &(dataGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["G2CTransferUpdate"] = &(g2cTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["G2CTransferGen"] = &(g2cTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["C2CTransferUpdate"] = &(c2cTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["C2CTransferGen"] = &(c2cTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["Reaper"] = &(reaper->mUpdateDurationSummed);

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
                for (std::unique_ptr<gcp::CBucket>& bucket : region->mStorageElements)
                    g2cTransferGen->mDstStorageElements.push_back(bucket.get());
            }
        }
        mSchedule.emplace(new CBillingGenerator(this));
    }
    g2cTransferGen->mSrcStorageElements = gridStoragleElements;
    dataGen->mStorageElements = gridStoragleElements;

    c2cTransferGen->mSrcStorageElements = g2cTransferGen->mDstStorageElements;
    c2cTransferGen->mDstStorageElements = g2cTransferGen->mDstStorageElements;

    for(std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
    {
        ok = InsertSite(gridSite.get());
        assert(ok);
        for(std::unique_ptr<CStorageElement>& storageElement : gridSite->mStorageElements)
        {
            ok = InsertStorageElement(storageElement.get());
            assert(ok);
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
        }
        for(std::unique_ptr<CLinkSelector>& linkselector : region->mLinkSelectors)
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
