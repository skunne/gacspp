
#include <atomic>					/* std::atomic */
#include <libpq-fe.h>
#include <experimental/filesystem>

class COutput
{
private:
    /* COutput() = default; */

    
    std::atomic_bool mIsConsumerRunning = false;
    PGconn *postGreConnection;

public:
    bool Initialise();
    void Shutdown();
    bool CreateTable(const std::string& tableName, const std::string& column);

};

