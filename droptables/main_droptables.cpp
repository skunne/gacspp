
#include <iostream>			/* display progress */
#include <libpq-fe.h>       /* PostgreSQL */

#define DROPTABLE	"DROP TABLE IF EXISTS "
#define CASCADE		" CASCADE;"

bool printStatus(PGresult *result)
{
	ExecStatusType status = PQresultStatus(result);

	bool empty_query = (status == PGRES_EMPTY_QUERY);
	bool bad_response = (status == PGRES_BAD_RESPONSE);
	bool fatal_error = (status == PGRES_FATAL_ERROR);
	bool everything_ok = !(empty_query || bad_response || fatal_error);

	std::cout << PQresStatus(status) << std::endl;
	if (!everything_ok)
		std::cout << PQresultErrorMessage(result);

	return everything_ok;
}

int main(void)
{
	std::string tablenames[] = {"helloworld", "files", "linkselectors", "replicas", "sites", "storageelements", "transfers"};
	PGconn *conn = PQconnectdb("user=admin host=dbod-skunne-testing.cern.ch port=6601 dbname=postgres");
	for (std::string name : tablenames)
	{
    	const std::string str = DROPTABLE + name + CASCADE;
    	std::cout << std::endl << str << std::endl;
    	PGresult *result = PQexec(conn, str.c_str());
    	printStatus(result);
	}
	PQfinish(conn);

	return (0);
}
