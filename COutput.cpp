#include <cassert>
#include <chrono>
#include <fstream>
#include <iomanip>        /* std::put_time() */
#include <sstream>
#include <algorithm>        /* std::count() counting number of params in a query */
#include <cstdio>           /* snprintf for IBindableValue::tostring() */

#include <iostream>         /* DEBUG */

#include <libpq-fe.h>       /* PostgreSQL */

#include "constants.h"
#include "COutput.hpp"      /* COutput, struct Statement, CInsertStatements, IBindableValue */
//#include "sqlite3.h"

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
    /*bool Bind(sqlite3_stmt* const stmt, int idx) final
    {
        //return sqlite3_bind_double(stmt, idx, mValue) == SQLITE_OK;
        return false;
    }*/
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
    /*bool Bind(sqlite3_stmt* const stmt, int idx) final
    {
        //return sqlite3_bind_int(stmt, idx, mValue) == SQLITE_OK;
        return false;
    }*/
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
    /*bool Bind(sqlite3_stmt* const stmt, int idx) final
    {
        //return sqlite3_bind_int64(stmt, idx, mValue) == SQLITE_OK;
        return false;
    }*/
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
    /*bool Bind(sqlite3_stmt* const stmt, int idx) final
    {
        //if(mValue.empty())
        //    return sqlite3_bind_null(stmt, idx) == SQLITE_OK;
        //else
        //    return sqlite3_bind_text(stmt, idx, mValue.c_str(), mValue.size(), SQLITE_TRANSIENT) == SQLITE_OK;
        return false;
    }*/
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
    const int numToBindPerRow = stmt->nParams;

    /*
    ** DEBUG
    */
    /*std::cout << std::endl;
    std::cout << "stmt:         " << stmt->id << std::endl;
    PGresult *description = PQdescribePrepared(conn, std::to_string(stmt->id).c_str());
    PQprintOpt printopt = { true, true, true, true, true, true };
    PQprint(stdout, description, &printopt);
    fprintf(stdout, "\nhello\n");
    std::cout << "nParams:      " << numToBindPerRow << std::endl;
    std::cout << "mValues.size: " << mValues.size() << std::endl;
    //mValues.size(), mPreparedStatementIdx, and the query with its index in AddPreparedStatement()*/

    assert(numToBindPerRow > 0);
    assert((mValues.size() % numToBindPerRow) == 0);

    // return value: counting the total number of elements inserted in the table
    // == numToBindPerRow * nbRows
    std::size_t numInserted = 0;

    // parameters for PQexecPrepared
    std::string stmtName = std::to_string(stmt->id);//std::to_string(n);  // where to find n ??
    int nParams = stmt->nParams;
    char *paramValuesArray = (char *) malloc(nParams * MAX_PARAM_LENGTH * sizeof(char));  //strings representing params
    char **paramValues = (char **) malloc(nParams * sizeof(char *));  //pointers to paramValuesArray for PQexecPrepared()
    if (paramValuesArray == NULL || paramValues == NULL)
    {
        std::cout << "malloc() failure for paramValues or paramValuesArray" << std::endl;
        std::cout << "    paramValuesArray [" << paramValuesArray << ']' << std::endl;
        std::cout << "    paramValues      [" << paramValues << ']' << std::endl;
    }
    int *paramLengths = NULL;   // ignored for text-format parameters
    int *paramFormats = NULL;   // NULL means all params are strings, which is maybe not optimal but ok
    int resultFormat = 0;       // text; change to 1 for binary

    // make paramValues point to paramValuesArray
    for (int numBinded = 0; numBinded<numToBindPerRow; ++numBinded)
        paramValues[numBinded] = &(paramValuesArray[numBinded * MAX_PARAM_LENGTH]);

    // iterate over all queries in mValues
    auto curValsIt = mValues.begin();
    while(curValsIt != mValues.end())
    {
        // retrieve parameters and store them in paramValuesArray
        for(int numBinded=0; numBinded<numToBindPerRow; ++numBinded)
        {
            (*curValsIt)->tostring(paramValues[numBinded]);
            ++curValsIt;
        }

        // execute statement with parameters
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
    bool connectionOK;
    // connect to database
    //this->postGreConnection = PQconnectdb("user=admin host=dbod-skunne-testing.cern.ch port=6601 dbname=postgres");
    this->postGreConnection = PQconnectdb("user=postgres host=localhost port=5432 dbname=postgres");

    // change settings to populate faster
    const std::string autocommit_off = "SET AUTOCOMMIT TO OFF;";
    //const std::string "SET wal_level TO minimal; SET archive_mode TO off; SET max_wal_senders TO zero";
    
    connectionOK = this->postGreConnection != NULL;
    //return (this->postGreConnection != NULL && PQexec(postGreConnection, autocommit_off.c_str()) != NULL);
    
    PQexec(postGreConnection, autocommit_off.c_str());

    PQexec(postGreConnection, "BEGIN TRANSACTION");

    return(connectionOK);
    //disable autocommit
    //increase maintenance_work_mem
    //increase checkpoint_segments
    //setting wal_level to minimal, archive_mode to off, and max_wal_senders to zero. But note that changing these settings requires a server restart.
    //return (this->postGreConnection != NULL);  /* Although the doc doesn't say anything about failure */
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
    PQexec(postGreConnection, "COMMIT TRANSACTION");
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

// prepare a query with placeholders
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

    // prepare the statement with name stmtName
    PQprepare(postGreConnection, stmtName.c_str(), statementString.c_str(), nParams, paramTypes);

    /*
    ** DEBUG
    */
    /*std::cout << "Preparing statement: " << stmtName << std::endl;
    std::cout << "    " << statementString << std::endl;*/

    // save number of prepared statements
    struct Statement stmt = { .id = nbPreparedStatements, .nParams = nParams };
    mPreparedStatements.emplace_back(stmt);
    nbPreparedStatements += 1;
    return nbPreparedStatements - 1;
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
    //PQexec(postGreConnection, "BEGIN TRANSACTION");
    //std::size_t numInsertedCurTransaction = 0;    // BEGIN/COMMIT optimisation
    while (mIsConsumerRunning || mConsumerIdx != mProducerIdx)
    {
        while (mConsumerIdx != mProducerIdx)
        {
            //if(numInsertedCurTransaction > 25000)
            //{
            //    PQexec(postGreConnection, "COMMIT TRANSACTION; BEGIN TRANSACTION");
            //    numInsertedCurTransaction = 0;
            //}
            // ConsumerThread() takes an CInsertStatements object from the queue
            const std::size_t sqlStmtIdx = mStatementsBuffer[mConsumerIdx]->GetPreparedStatementIdx();
            struct Statement stmt = mPreparedStatements[sqlStmtIdx];    // get name and number of arguments

            /*
            ** DEBUG
            */
            /*std::cout << std::endl;
            std::cout << "nbPreparedStatements " << nbPreparedStatements << std::endl;
            std::cout << "number of queries:   " << (mProducerIdx - mConsumerIdx) % OUTPUT_BUF_SIZE << std::endl;*/

            // Bind parameters and execute query
            //numInsertedCurTransaction += mStatementsBuffer[mConsumerIdx]->BindAndInsert(postGreConnection, &stmt);
            mStatementsBuffer[mConsumerIdx]->BindAndInsert(postGreConnection, &stmt);

            // delete from queue
            mStatementsBuffer[mConsumerIdx] = nullptr;
            mConsumerIdx = (mConsumerIdx + 1) % OUTPUT_BUF_SIZE;
        }

        // try to use time while buf is empty by commiting the transactionn
        //if(numInsertedCurTransaction > 1000)
        //{
        //    PQexec(postGreConnection, "COMMIT TRANSACTION; BEGIN TRANSACTION");

        //    numInsertedCurTransaction = 0;
        //}
        //else
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::cout << "The queue is empty!!" << std::endl;
    }
    //PQexec(postGreConnection, "COMMIT TRANSACTION");

    // 1 ConsumerThread() takes an CInsertStatements object from the queue
    // 2 CInsertStatement::BindAndInsert() is called by the consumer thread
    // 3 CInsertStatement::BindAndInsert() knows the prepared query index/name (CInsertStatement::mPreparedStatementIdx) and the parameter values (CInsertStatement::mValues)
    // 4 CInsertStatement::BindAndInsert() determines the number N of parameters per insert-into statement (=number of columns of the table)
    // 5 CInsertStatement::BindAndInsert() calls IBindableValue::Bind() for the next N values in CInsertStatement::mValues
    // 6 IBindableValue::Bind() is implemented by the appropriate subclasses (in your case the function will add the value stored by the object of the subclass of IBindableValue to the parameter list for the psql API)
    // 7 CInsertStatement::BindAndInsert() executes the prepared statement
    // 8 Point 5 is repeated until all values in CInsertStatement::mValues have been touched
}
