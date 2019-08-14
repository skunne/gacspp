#pragma once

#include <atomic>
#include <experimental/filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <libpq-fe.h>   /* postgreSQL api */

#define OUTPUT_BUF_SIZE 8192

struct sqlite3;
struct sqlite3_stmt;


class IBindableValue
{
public:
    virtual void tostring(char *str) = 0;
    virtual bool Bind(sqlite3_stmt* stmt, int idx) = 0;
};


class CInsertStatements
{
protected:
    std::size_t mPreparedStatementIdx = 0;
    std::vector<std::unique_ptr<IBindableValue>> mValues;

public:
    CInsertStatements(std::size_t preparedStatementIdx, std::size_t numReserve=0);

    inline auto GetPreparedStatementIdx() const -> std::size_t
    {return mPreparedStatementIdx;}

    bool IsEmpty() const
    {return mValues.empty();}

    void AddValue(double value);
    void AddValue(int value);
    void AddValue(std::uint32_t value);
    void AddValue(std::uint64_t value);
    void AddValue(const std::string& value);
    void AddValue(std::string&& value);

    auto BindAndInsert(PGconn *conn, struct Statement const *stmt) -> std::size_t;
};

struct Statement
{
    std::size_t nb;
    int         nParams;
};

class COutput
{
private:
    COutput() = default;

    std::atomic_bool mIsConsumerRunning = false;
    std::thread mConsumerThread;

    std::atomic_size_t mConsumerIdx = 0;
    std::atomic_size_t mProducerIdx = 0;
    std::unique_ptr<CInsertStatements> mStatementsBuffer[OUTPUT_BUF_SIZE];

    sqlite3* mDB = nullptr;
    //std::vector<sqlite3_stmt*> mPreparedStatements;   // not used by postgres
    std::vector<struct Statement> mPreparedStatements;
    std::size_t nbPreparedStatements = 0;

    std::experimental::filesystem::path mDBFilePath;

    PGconn *postGreConnection;

public:
    COutput(const COutput&) = delete;
    COutput& operator=(const COutput&) = delete;
    COutput(const COutput&&) = delete;
    COutput& operator=(const COutput&&) = delete;

    ~COutput();

    static auto GetRef() -> COutput&;
    static void LogCallback(void* data, int errorCode, const char* errorMessage);

    bool Initialise();
    bool StartConsumer();
    void Shutdown();

    void replaceQuestionMarks(std::string& statementString);
    auto AddPreparedSQLStatement(const std::string& queryString) -> std::size_t;
    bool CreateTable(const std::string& tableName, const std::string& column);
    bool InsertRow(const std::string& tableName, const std::string& row);

    void QueueInserts(std::unique_ptr<CInsertStatements>&& statements);

    void ConsumerThread();
};
