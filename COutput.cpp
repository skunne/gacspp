#include <cassert>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <algorithm>        /* std::count() counting number of params in a query */

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
    //bool Bind(char **paramValues, int *paramLengths, size_t idx)
    bool Bind(sqlite3_stmt* const stmt, int idx) final
    {
        std::string param = std::to_string(mValue);
        //paramValues[idx] = new char[param.size() + 1];
        //param.copy(paramValues[idx], param.size() + 1);
        //paramLengths[idx] = param.size();
        return (true);  // why??
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

std::size_t CInsertStatements::BindAndInsert(struct Statement const *stmt)
{
    if(mValues.empty())
        return 0;

    //const std::size_t numToBindPerRow = static_cast<std::size_t>(sqlite3_bind_parameter_count(stmt));
    const std::size_t numToBindPerRow = stmt->nParams;  // maybe?

    assert(numToBindPerRow > 0);
    assert((mValues.size() % numToBindPerRow) == 0);

    auto curValsIt = mValues.begin();
    std::size_t numInserted = 0;
    std::string stmtName = std::to_string(stmt->nb);//std::to_string(n);  // where to find n ??
    while(curValsIt != mValues.end())
    {
        int nParams = stmt->nParams;
        char **paramValues = NULL;  // get them somewhere from BindableValue??
        int *paramLengths = NULL;   //??
        int *paramFormats = NULL;   // NULL means all params are strings, which is maybe not optimal but ok
        int resultFormat = 0;       // text; change to 1 for binary
        for(std::size_t numBinded=1; numBinded<=numToBindPerRow; ++numBinded)
        {
            //(*curValsIt)->Bind(paramValues, paramLengths);
            ++curValsIt;
        }
        //sqlite3_step(stmt);
        //sqlite3_clear_bindings(stmt);
        //sqlite3_reset(stmt);
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

/*
** Transform "Hello ? World ?" into "Hello $1 World $2"
*/
void COutput::replaceQuestionMarks(std::string& statementString)
{
    // count the '?' as they are replaced
    std::size_t n = 1;
    // find first '?'
    std::size_t index = statementString.find('?');
    // while there remains a '?' in statementString
    while (index != std::string::npos)
    {
        // build the "$n" substring that will replace '?'
        std::string str = "$n";
        str.replace(1, 1, std::to_string(n));
        // replace '?' with "$n"
        statementString.replace(index, 1, str);
        // count the '?' so the next substring is "$(n+1)"
        ++n;
        // find next '?'
        index = statementString.find('?');
    }
}

auto COutput::AddPreparedSQLStatement(const std::string& queryString) -> std::size_t
{
    // make a local copy of the string, that will be edited to replace '?' with "$n"
    std::string statementString(queryString);

    // arguments for PQprepare
    std::string stmtName = std::to_string(nbPreparedStatements);
    int nParams = std::count(statementString.begin(), statementString.end(), '?');
    const Oid *paramTypes = NULL;   // ???

    // replace "? ? ?" placeholders with "$1 $2 $3" in query string
    replaceQuestionMarks(statementString);

    PQprepare(postGreConnection, stmtName.c_str(), statementString.c_str(), nParams, paramTypes);

    // save number of prepared statements
    struct Statement stmt = { .nb = nbPreparedStatements, .nParams = nParams };
    mPreparedStatements.emplace_back(stmt);
    nbPreparedStatements += 1;
    return nbPreparedStatements;
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
    PGresult *result = PQexec(postGreConnection, str.c_str());
    ExecStatusType resultStatus = PQresultStatus(result);
    return (resultStatus != PGRES_BAD_RESPONSE && resultStatus != PGRES_FATAL_ERROR);
    /* this return shows errors but hides warnings */
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
    std::size_t numInsertedCurTransaction = 0;
    while (mConsumerIdx != mProducerIdx)
    {
        // ConsumerThread() takes an CInsertStatements object from the queue
        const std::size_t sqlStmtIdx = mStatementsBuffer[mConsumerIdx]->GetPreparedStatementIdx();
        //sqlite3_stmt* sqlStmt = mPreparedStatements[sqlStmtIdx];
        struct Statement stmt = mPreparedStatements[sqlStmtIdx];    // get name and number of arguments
        numInsertedCurTransaction += mStatementsBuffer[mConsumerIdx]->BindAndInsert(&stmt);


    }
    

    //CInsertStatement::BindAndInsert() is called by the consumer thread
    //CInsertStatement::BindAndInsert() knows the prepared query index/name (CInsertStatement::mPreparedStatementIdx) and the parameter values (CInsertStatement::mValues)
    //CInsertStatement::BindAndInsert() determines the number N of parameters per insert-into statement (=number of columns of the table)
    //CInsertStatement::BindAndInsert() calls IBindableValue::Bind() for the next N values in CInsertStatement::mValues
    //IBindableValue::Bind() is implemented by the appropriate subclasses (in your case the function will add the value stored by the object of the subclass of IBindableValue to the parameter list for the psql API)
    //CInsertStatement::BindAndInsert() executes the prepared statement
    //Point 5 is repeated until all values in CInsertStatement::mValues have been touched
}
/*

void COutput::GetParams(char const *stmtName, int *nParams, char ***paramValues, int **paramLengths, int **paramFormats)
{
    PGresult *description = PQdescribePrepared(postGreConnection, stmtName);
    *nParams = PQnfields(description);  // not so sure this is correct
    
}
void COutput::badConsumerThread()
{
    PQexec(postGreConnection, "BEGIN TRANSACTION");
    std::size_t numInsertedCurTransaction = 0;
    for (size_t n = 0; n < nbPreparedStatements; ++n)
    {
        if(numInsertedCurTransaction > 25000)
        {
            PQexec(postGreConnection, "END TRANSACTION; BEGIN TRANSACTION");
            numInsertedCurTransaction = 0;
        }
        // arguments for PQexecPrepared
        std::string stmtName = std::to_string(n);
        int nParams = 0;            //??
        char **paramValues = NULL;  // get them somewhere from BindableValue??
        int *paramLengths = NULL;   //??
        int *paramFormats = NULL;   // NULL means all params are strings, which is maybe not optimal but ok
        int resultFormat = 0;       // text; change to 1 for binary
        GetParams(stmtName.c_str(), &nParams, &paramValues, &paramLengths, &paramFormats);


        //numInsertedCurTransaction += mStatementsBuffer[mConsumerIdx]->BindAndInsert(sqlStmt);
        //mStatementsBuffer[mConsumerIdx] = nullptr;
        //mConsumerIdx = (mConsumerIdx + 1) % OUTPUT_BUF_SIZE;


        // execute one prepared statement
        PQexecPrepared(postGreConnection, stmtName.c_str(), nParams, paramValues, paramLengths, paramFormats, resultFormat);   
    }

    PQexec(postGreConnection, "END TRANSACTION");
}
*/
/*
void COutput::oldConsumerThread()
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
                //PQexec(postGreConnection, "END TRANSACTION; BEGIN TRANSACTION");
                numInsertedCurTransaction = 0;
            }

            const std::size_t sqlStmtIdx = mStatementsBuffer[mConsumerIdx]->GetPreparedStatementIdx();
            sqlite3_stmt* sqlStmt = mPreparedStatements[sqlStmtIdx];
            numInsertedCurTransaction += mStatementsBuffer[mConsumerIdx]->BindAndInsert(sqlStmt);
            *
            ** PGresult *PQexecPrepared(PGconn *conn,
            **                          const char *stmtName,
            **                          int nParams,
            **                          const char * const *paramValues,
            **                          const int *paramLengths,
            **                          const int *paramFormats,
            **                          int resultFormat);
            *
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
*/
