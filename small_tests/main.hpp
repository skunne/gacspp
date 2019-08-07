
#include <atomic>					/* std::atomic */
#include <libpq-fe.h>
#include <experimental/filesystem>

class COutput
{
private:
    /* COutput() = default; */

    std::size_t nbPreparedStatements = 0;
    
    std::atomic_bool mIsConsumerRunning = false;

public:
    PGconn *postGreConnection;	// should be private; public for testing purposes
    bool Initialise();
    void Shutdown();
    bool CreateTable(const std::string& tableName, const std::string& column);
    bool InsertRow(const std::string& tableName, const std::string& row);
    void replaceQuestionMarks(std::string& statementString);
    auto AddPreparedSQLStatement(const std::string& queryString) -> std::size_t;

};

