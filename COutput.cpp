#include <cassert>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>

#include "COutput.hpp"
#include "sqlite3.h"


auto COutput::GetRef() -> COutput&
{
    static COutput mInstance;
    return mInstance;
}

void COutput::LogCallback(void* dat, int errorCode, const char* errorMessage)
{
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::stringstream timeStr;
    timeStr << std::put_time(std::localtime(&now), "%H-%M-%S");
    static std::ofstream sqliteLog(timeStr.str() + ".log");
    sqliteLog << "[" << timeStr.str() << "] - " << errorCode << ": " << errorMessage << std::endl;
}

COutput::~COutput()
{
    Shutdown();
}

bool COutput::Initialise(const std::string& dbFileNamePath)
{
    assert(mDB == nullptr);

    if(sqlite3_config(SQLITE_CONFIG_LOG, COutput::LogCallback, nullptr) != SQLITE_OK)
        return false;

    sqlite3_open_v2(dbFileNamePath.c_str(), &mDB, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
    if(mDB == nullptr)
        return false;


    return true;
}

bool COutput::StartConsumer()
{
    if(mConsumerThread.joinable())
        return false;

    mIsConsumerRunning = true;
    mConsumerThread = std::thread(&COutput::ConsumerThread, this);

    return true;
}

void COutput::Shutdown()
{
    mIsConsumerRunning = false;
    if(mConsumerThread.joinable())
        mConsumerThread.join();

    while(!mPreparedStatements.empty())
    {
        sqlite3_finalize(mPreparedStatements.back());
        mPreparedStatements.pop_back();
    }

    if(mDB != nullptr)
    {
        sqlite3_close(mDB);
        mDB = nullptr;
    }
}

auto COutput::AddPreparedSQLStatement(const std::string& statementString) -> std::size_t
{
    sqlite3_stmt* preparedStatement;
    sqlite3_prepare_v3(mDB, statementString.c_str(), statementString.size() + 1, SQLITE_PREPARE_PERSISTENT, &preparedStatement, nullptr);
    assert(preparedStatement != nullptr);
    mPreparedStatements.emplace_back(preparedStatement);
    return (mPreparedStatements.size() - 1);
}

bool COutput::CreateTable(const std::string& tableName, const std::string& columns)
{
    if(mIsConsumerRunning)
        return false;
    const std::string str = "CREATE TABLE " + tableName + "(" + columns + ");";
    return sqlite3_exec(mDB, str.c_str(), nullptr, nullptr, nullptr) == SQLITE_OK;
}

bool COutput::InsertRow(const std::string& tableName, const std::string& row)
{
    if(mIsConsumerRunning)
        return false;
    const std::string str = "INSERT INTO " + tableName + " VALUES (" + row + ");";
    return sqlite3_exec(mDB, str.c_str(), nullptr, nullptr, nullptr) == SQLITE_OK;
}

void COutput::QueueStatement(IConcreteStatement* statement)
{
    std::size_t newProducerIdx = (mProducerIdx + 1) % OUTPUT_BUF_SIZE;
    while(newProducerIdx == mConsumerIdx)
    {
        assert(mIsConsumerRunning);
        newProducerIdx = (mProducerIdx + 1) % OUTPUT_BUF_SIZE;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    mStatementBuffer[mProducerIdx] = std::unique_ptr<IConcreteStatement>(statement);
    mProducerIdx = newProducerIdx;
}

void COutput::ConsumerThread()
{
    bool hasTransactionBegun = false;
    std::size_t numInsertionsPerTransaction = 0;
    while(mIsConsumerRunning || (mConsumerIdx != mProducerIdx))
    {
        while(mConsumerIdx != mProducerIdx)
        {
            if(!hasTransactionBegun)
            {
                sqlite3_exec(mDB, "BEGIN TRANSACTION", nullptr, nullptr, nullptr);
                hasTransactionBegun = true;
            }
            std::unique_ptr<IConcreteStatement> statement = std::move(mStatementBuffer[mConsumerIdx]);
            mConsumerIdx = (mConsumerIdx + 1) % OUTPUT_BUF_SIZE;
            assert(statement != nullptr);
            auto sqlStmt = mPreparedStatements[statement->GetPreparedStatementIdx()];
            numInsertionsPerTransaction += statement->BindAndExecute(sqlStmt);
            if(numInsertionsPerTransaction > 20000)
            {
                sqlite3_exec(mDB, "END TRANSACTION", nullptr, nullptr, nullptr);
                numInsertionsPerTransaction = 0;
                hasTransactionBegun = false;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if(hasTransactionBegun)
        sqlite3_exec(mDB, "END TRANSACTION", nullptr, nullptr, nullptr);
}
