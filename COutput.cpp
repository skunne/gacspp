#include <cassert>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <algorithm>        /* std::count() counting number of params in a query */
#include <cstdio>           /* snprintf for IBindableValue::tostring() */

#include <libpq-fe.h>       /* PostgreSQL */

#include "constants.h"
#include "COutput.hpp"
#include "sqlite3.h"

#define MAX_PARAM_LENGTH 100

class CBindableDoubleValue : public IBindableValue
{
private:
    double mValue;

public:
    CBindableDoubleValue(double val)
        : mValue(val)
    {}

    void tostring(char *str)
    {
        snprintf(str, MAX_PARAM_LENGTH, "%f", mValue);
    }
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

    void tostring(char *str)
    {
        snprintf(str, MAX_PARAM_LENGTH, "%d", mValue);
    }
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

    void tostring(char *str)
    {
        snprintf(str, MAX_PARAM_LENGTH, "%llu", mValue);
    }
    //bool Bind(char **paramValues, int *paramLengths, size_t idx)
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

    void tostring(char *str)
    {
        snprintf(str, MAX_PARAM_LENGTH, "%s", mValue.c_str());
    }
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

std::size_t CInsertStatements::BindAndInsert(PGconn *conn, struct Statement const *stmt)
{
    if(mValues.empty())
        return 0;

    //const std::size_t numToBindPerRow = static_cast<std::size_t>(sqlite3_bind_parameter_count(stmt));
    const std::size_t numToBindPerRow = stmt->nParams;  // maybe?

    assert(numToBindPerRow > 0);
    assert((mValues.size() % numToBindPerRow) == 0);

    auto curValsIt = mValues.begin();
    std::size_t numInserted = 0;

    // parameters for PQexecPrepared
    std::string stmtName = std::to_string(stmt->nb);//std::to_string(n);  // where to find n ??
    int nParams = stmt->nParams;
    char *paramValuesArray = (char *) malloc(nParams * MAX_PARAM_LENGTH * sizeof(char));
    char **paramValues = (char **) malloc(nParams * sizeof(char *));
    int *paramLengths = NULL;   // ignored for text-format parameters
    int *paramFormats = NULL;   // NULL means all params are strings, which is maybe not optimal but ok
    int resultFormat = 0;       // text; change to 1 for binary

    for (std::size_t numBinded = 0; numBinded<numToBindPerRow; ++numBinded)
        paramValues[numBinded] = &(paramValuesArray[numBinded * MAX_PARAM_LENGTH]);
    while(curValsIt != mValues.end())
    {
        for(std::size_t numBinded=0; numBinded<numToBindPerRow; ++numBinded)
        {
            (*curValsIt)->tostring(paramValues[numBinded]);
            ++curValsIt;
        }
        PQexecPrepared(conn, stmtName.c_str(), nParams, paramValues, paramLengths, paramFormats, resultFormat);
        numInserted += 1;
    }

    free(paramValues);
    free(paramValuesArray);
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

/* initialise connection to postgres db */
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

/* Shutdown postgres connection */
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
    PQexec(postGreConnection, "BEGIN TRANSACTION");
    std::size_t numInsertedCurTransaction = 0;
    while (mIsConsumerRunning || mConsumerIdx != mProducerIdx)
    {
        while (mConsumerIdx != mProducerIdx)
        {
            if(numInsertedCurTransaction > 25000)
            {
                PQexec(postGreConnection, "END TRANSACTION; BEGIN TRANSACTION");
                numInsertedCurTransaction = 0;
            }
            // ConsumerThread() takes an CInsertStatements object from the queue
            const std::size_t sqlStmtIdx = mStatementsBuffer[mConsumerIdx]->GetPreparedStatementIdx();
            struct Statement stmt = mPreparedStatements[sqlStmtIdx];    // get name and number of arguments

            // Bind parameters and execute query
            numInsertedCurTransaction += mStatementsBuffer[mConsumerIdx]->BindAndInsert(postGreConnection, &stmt);

            // delete from queue
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
    PQexec(postGreConnection, "END TRANSACTION");

    // 1 ConsumerThread() takes an CInsertStatements object from the queue
    // 2 CInsertStatement::BindAndInsert() is called by the consumer thread
    // 3 CInsertStatement::BindAndInsert() knows the prepared query index/name (CInsertStatement::mPreparedStatementIdx) and the parameter values (CInsertStatement::mValues)
    // 4 CInsertStatement::BindAndInsert() determines the number N of parameters per insert-into statement (=number of columns of the table)
    // 5 CInsertStatement::BindAndInsert() calls IBindableValue::Bind() for the next N values in CInsertStatement::mValues
    // 6 IBindableValue::Bind() is implemented by the appropriate subclasses (in your case the function will add the value stored by the object of the subclass of IBindableValue to the parameter list for the psql API)
    // 7 CInsertStatement::BindAndInsert() executes the prepared statement
    // 8 Point 5 is repeated until all values in CInsertStatement::mValues have been touched
}
