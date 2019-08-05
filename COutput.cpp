#include <cassert>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>

#include <libpq-fe.h>       /* PostgreSQL */

#include "constants.h"
#include "COutput.hpp"
#include "sqlite3.h"



class CBindableDoubleValue : public IBindableValue
{
private:
    double mValue;

public:
    CBindableDoubleValue(double val)
        : mValue(val)
    {}

    bool Bind(sqlite3_stmt* const stmt, int idx) final
    {
        return sqlite3_bind_double(stmt, idx, mValue) == SQLITE_OK;
    }
};

class CBindableIntValue : public IBindableValue
{
private:
    int mValue;

public:
    CBindableIntValue(int val)
        : mValue(val)
    {}

    bool Bind(sqlite3_stmt* const stmt, int idx) final
    {
        return sqlite3_bind_int(stmt, idx, mValue) == SQLITE_OK;
    }
};

class CBindableInt64Value : public IBindableValue
{
private:
    uint64_t mValue;

public:
    CBindableInt64Value(std::uint64_t val)
        : mValue(val)
    {}

    bool Bind(sqlite3_stmt* const stmt, int idx) final
    {
        return sqlite3_bind_int64(stmt, idx, mValue) == SQLITE_OK;
    }
};

class CBindableStringValue : public IBindableValue
{
private:
    std::string mValue;

public:
    CBindableStringValue(const std::string& val)
        : mValue(val)
    {}

    CBindableStringValue(std::string&& val)
        : mValue(std::move(val))
    {}

    bool Bind(sqlite3_stmt* const stmt, int idx) final
    {
        if(mValue.empty())
            return sqlite3_bind_null(stmt, idx) == SQLITE_OK;
        else
            return sqlite3_bind_text(stmt, idx, mValue.c_str(), mValue.size(), SQLITE_TRANSIENT) == SQLITE_OK;
    }
};

CInsertStatements::CInsertStatements(std::size_t preparedStatementIdx, std::size_t numReserve)
    : mPreparedStatementIdx(preparedStatementIdx)
{
    if(numReserve>0)
        mValues.reserve(numReserve);
}

void CInsertStatements::AddValue(double value)
{
    mValues.emplace_back(new CBindableDoubleValue(value));
}

void CInsertStatements::AddValue(int value)
{
    mValues.emplace_back(new CBindableIntValue(value));
}

void CInsertStatements::AddValue(std::uint32_t value)
{
    mValues.emplace_back(new CBindableInt64Value(value));
}

void CInsertStatements::AddValue(std::uint64_t value)
{
    mValues.emplace_back(new CBindableInt64Value(value));
}

void CInsertStatements::AddValue(const std::string& value)
{
    mValues.emplace_back(new CBindableStringValue(value));
}

void CInsertStatements::AddValue(std::string&& value)
{
    mValues.emplace_back(new CBindableStringValue(std::move(value)));
}

std::size_t CInsertStatements::BindAndInsert(sqlite3_stmt* const stmt)
{
    if(mValues.empty())
        return 0;

    const std::size_t numToBindPerRow = static_cast<std::size_t>(sqlite3_bind_parameter_count(stmt));
    assert(numToBindPerRow > 0);
    assert((mValues.size() % numToBindPerRow) == 0);

    auto curValsIt = mValues.begin();
    std::size_t numInserted = 0;
    while(curValsIt != mValues.end())
    {
        for(std::size_t numBinded=1; numBinded<=numToBindPerRow; ++numBinded)
        {
            (*curValsIt)->Bind(stmt, numBinded);
            ++curValsIt;
        }
        sqlite3_step(stmt);
        sqlite3_clear_bindings(stmt);
        sqlite3_reset(stmt);
        /*PQexecPrepared(PGconn *conn,
                         const char *stmtName,
                         int nParams,
                         const char * const *paramValues,
                         const int *paramLengths,
                         const int *paramFormats,
                         int resultFormat);*/
        numInserted += 1;
    }

    mValues.clear();
    return numInserted;
}


auto COutput::GetRef() -> COutput&
{
    static COutput mInstance;
    return mInstance;
}

void COutput::LogCallback(void* dat, int errorCode, const char* errorMessage)
{
    (void)dat;
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::stringstream timeStr;
    timeStr << std::put_time(std::localtime(&now), "%y%j_%H%M%S");
#ifdef STATIC_DB_LOG_NAME
    static std::ofstream sqliteLog(STATIC_DB_LOG_NAME);
#else
    static std::ofstream sqliteLog(timeStr.str() + ".log");
#endif
    sqliteLog << "[" << timeStr.str() << "] - " << errorCode << ": " << errorMessage << std::endl;
}

COutput::~COutput()
{
    Shutdown();
}

/* sql initialise connection to db */
/*
bool COutput::Initialise(const std::experimental::filesystem::path& dbFilePath, bool keepInMemory)
{
    assert(mDB == nullptr);

    if(sqlite3_config(SQLITE_CONFIG_LOG, COutput::LogCallback, nullptr) != SQLITE_OK)
        return false;

    if(keepInMemory)
    {
        mDBFilePath = dbFilePath;
        sqlite3_open_v2(":memory:", &mDB, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
    }
    else
        sqlite3_open_v2(dbFilePath.c_str(), &mDB, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);

    if(mDB == nullptr)
        return false;

    return true;
}
*/

/* postgre initialise connection to db */
bool COutput::Initialise(void)
{
    this->postGreConnection = PQconnectdb("user=admin host=dbod-skunne-testing.cern.ch port=6601 dbname=postgres");
    return (this->postGreConnection != NULL);  /* Although the doc doesn't say anything about failure */
    //return (true);
}

bool COutput::StartConsumer()
{
    if(mConsumerThread.joinable())
        return false;

    mIsConsumerRunning = true;
    mConsumerThread = std::thread(&COutput::ConsumerThread, this);

    return true;
}

/* SQLite3 Shutdown connection */
/*
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
        if (!mDBFilePath.empty())
        {
            sqlite3* diskDB;
            int ok = sqlite3_open(mDBFilePath.c_str(), &diskDB);
            if (ok == SQLITE_OK)
            {
                sqlite3_backup* backup = sqlite3_backup_init(diskDB, "main", mDB, "main");
                if (backup)
                {
                    sqlite3_backup_step(backup, -1);
                    sqlite3_backup_finish(backup);
                }
                sqlite3_close(diskDB);
            }
        }
        sqlite3_close(mDB);
        mDB = nullptr;
    }
}
*/

/* postgre Shutdown connection */
void COutput::Shutdown(void)
{
    PQfinish(postGreConnection);
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
    if(mIsConsumerRunning)     /* I do not know what this does? */
        return false;
    const std::string str = "CREATE TABLE " + tableName + "(" + columns + ");";
    return (PQexec(postGreConnection, str.c_str()) != NULL);
}

bool COutput::InsertRow(const std::string& tableName, const std::string& row)
{
    if(mIsConsumerRunning)
        return false;
    const std::string str = "INSERT INTO " + tableName + " VALUES (" + row + ");";
    return (PQexec(postGreConnection, str.c_str()) != NULL);
}

void COutput::QueueInserts(std::unique_ptr<CInsertStatements>&& statements)
{
    assert(statements != nullptr);

    if(statements->IsEmpty())
        return;

    std::size_t newProducerIdx = (mProducerIdx + 1) % OUTPUT_BUF_SIZE;
    while(newProducerIdx == mConsumerIdx)
    {
        assert(mIsConsumerRunning);
        newProducerIdx = (mProducerIdx + 1) % OUTPUT_BUF_SIZE;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    mStatementsBuffer[mProducerIdx] = std::move(statements);
    mProducerIdx = newProducerIdx;
}

void COutput::ConsumerThread()
{
    sqlite3_exec(mDB, "BEGIN TRANSACTION", nullptr, nullptr, nullptr);

    std::size_t numInsertedCurTransaction = 0;
    while(mIsConsumerRunning || (mConsumerIdx != mProducerIdx))
    {
        while(mConsumerIdx != mProducerIdx)
        {
            if(numInsertedCurTransaction > 25000)
            {
                sqlite3_exec(mDB, "END TRANSACTION; BEGIN TRANSACTION", nullptr, nullptr, nullptr);
                numInsertedCurTransaction = 0;
            }

            const std::size_t sqlStmtIdx = mStatementsBuffer[mConsumerIdx]->GetPreparedStatementIdx();
            sqlite3_stmt* sqlStmt = mPreparedStatements[sqlStmtIdx];
            numInsertedCurTransaction += mStatementsBuffer[mConsumerIdx]->BindAndInsert(sqlStmt);

            mStatementsBuffer[mConsumerIdx] = nullptr;
            mConsumerIdx = (mConsumerIdx + 1) % OUTPUT_BUF_SIZE;
        }

        // try to use time while buf is empty by commiting the transactionn
        if(numInsertedCurTransaction > 1000)
        {
            sqlite3_exec(mDB, "END TRANSACTION; BEGIN TRANSACTION", nullptr, nullptr, nullptr);
            numInsertedCurTransaction = 0;
        }
        else
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    sqlite3_exec(mDB, "END TRANSACTION", nullptr, nullptr, nullptr);
}
