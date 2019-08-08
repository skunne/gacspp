#include <iostream>

#include <libpq-fe.h>       /* PostgreSQL */

#include "main.hpp"

bool printStatus(PGresult *result)
{
	ExecStatusType status = PQresultStatus(result);

	bool empty_query = (status == PGRES_EMPTY_QUERY);
	bool bad_response = (status == PGRES_BAD_RESPONSE);
	bool fatal_error = (status == PGRES_FATAL_ERROR);
	bool everything_ok = !(empty_query || bad_response || fatal_error);

	std::cout << "    Status: " << PQresStatus(status) << std::endl;
	if (!everything_ok)
		std::cout << PQresultErrorMessage(result);
/*
	std::cout << "    Status: "
			  << (empty_query ?
			  		"empty_query"
			  		: (bad_response ?
			  			"bad_response"
			  			: (fatal_error ? "fatal_error" : "everything ok")))
			  << std::endl;
*/

	return everything_ok;
}

int main(void)
{
	COutput coutput;

	std::cout << std::boolalpha;	// Print booleans as "true" and "false"

	std::cout << "  Opening connection..." << std::endl;
	std::cout << "    Connection with success: " << coutput.Initialise() << std::endl << std::endl;

	std::cout << "  Creating Table..." << std::endl;
	std::cout << "    Table 'helloworld' created with success: "
			  << coutput.CreateTable("helloworld", "id serial PRIMARY KEY, name VARCHAR (50)")
			  << std::endl << std::endl;

	std::cout << "  Inserting Row..." << std::endl;
	std::cout << "    Inserted three rows (12, 'Asterix'), (14, 'Idefix'), (15, 'Assurancetourix')" << std::endl;
	std::cout << "      Success: "
			  << coutput.InsertRow("helloworld(id, name)", "12, 'Asterix'") << ','
			  << coutput.InsertRow("helloworld", "14, 'Idefix'") << ','
			  << coutput.InsertRow("helloworld", "15, 'Assurancetourix'")
			  << std::endl << std::endl;

	std::cout << "  Preparing 4 Statements: no params, 1 param, 1 param, 2 params..." << std::endl;
	coutput.AddPreparedSQLStatement("SELECT * FROM helloworld");
	coutput.AddPreparedSQLStatement("SELECT ? FROM helloworld");
	coutput.AddPreparedSQLStatement("SELECT * FROM ?");
	std::size_t n = coutput.AddPreparedSQLStatement("SELECT ? FROM ?");
	std::cout << "    " << n << " Done." << std::endl << std::endl;

	std::cout << "  Executing previously prepared statements..." << std::endl;
	char const *paramValues[] = {"*", "helloworld"};
	int paramLengths[] = {2 * sizeof(char), 11 * sizeof(char)};
	//PQexecPrepared(PGconn *conn, stmtName, nParams, paramValues, paramLengths, paramFormats, resultFormat);
	printStatus(PQexecPrepared(coutput.postGreConnection, "0", 0, NULL, NULL, NULL, 0));
	printStatus(PQexecPrepared(coutput.postGreConnection, "1", 1, paramValues, paramLengths, NULL, 0));
	printStatus(PQexecPrepared(coutput.postGreConnection, "2", 1, &(paramValues[1]), &(paramLengths[1]), NULL, 0));
	printStatus(PQexecPrepared(coutput.postGreConnection, "3", 2, paramValues, paramLengths, NULL, 0));

	std::cout << std::endl << "  Closing connection..." << std::endl;
	coutput.Shutdown();
	std::cout << "  All done." << std::endl;

	return 0;
}

