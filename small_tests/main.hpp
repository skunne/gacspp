#include <libpq-fe.h>
#include <experimental/filesystem>

class COutput
{
private:
    /* COutput() = default; */

    

    PGconn *postGreConnection;

public:
    bool Initialise();
    void Shutdown();
};

