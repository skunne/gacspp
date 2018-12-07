#pragma once

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#define OUTPUT_BUF_SIZE 8192

struct sqlite3;
struct sqlite3_stmt;

class IConcreteStatement
{
protected:
    std::size_t mPreparedStatementIdx = 0;

public:
    IConcreteStatement(std::size_t preparedStatementIdx)
        : mPreparedStatementIdx(preparedStatementIdx)
    {}
    inline auto GetPreparedStatementIdx() const -> std::size_t
    {return mPreparedStatementIdx;}

    virtual std::size_t BindAndExecute(sqlite3_stmt* const stmt) = 0;
};

class COutput
{
private:
    COutput(){}

    std::atomic_bool mIsConsumerRunning = false;
    std::thread mConsumerThread;

    std::atomic_size_t mConsumerIdx = 0;
    std::atomic_size_t mProducerIdx = 0;
    std::unique_ptr<IConcreteStatement> mStatementBuffer[OUTPUT_BUF_SIZE];

    sqlite3* mDB = nullptr;
    std::vector<sqlite3_stmt*> mPreparedStatements;

public:
    COutput(const COutput&) = delete;
    COutput& operator=(const COutput&) = delete;
    COutput(const COutput&&) = delete;
    COutput& operator=(const COutput&&) = delete;

    ~COutput();

    static auto GetRef() -> COutput&;
    static void LogCallback(void* data, int errorCode, const char* errorMessage);

    bool Initialise(const std::string& dbFileNamePath);
    bool StartConsumer();
    void Shutdown();

    auto AddPreparedSQLStatement(const std::string& queryString) -> std::size_t;
    bool CreateTable(const std::string& tableName, const std::string& column);
    bool InsertRow(const std::string& tableName, const std::string& row);

    void QueueStatement(IConcreteStatement* statement);

    void ConsumerThread();
};
