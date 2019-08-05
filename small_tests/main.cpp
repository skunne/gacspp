#include <iostream>

#include <libpq-fe.h>       /* PostgreSQL */

#include "main.hpp"

bool COutput::Initialise(void)
{
    this->postGreConnection = PQconnectdb("user=admin host=dbod-skunne-testing.cern.ch port=6601 dbname=postgres");
    return (this->postGreConnection != NULL);  /* Although the doc doesn't say anything about failure */
    //return (true);
}

void COutput::Shutdown(void)
{
	PQfinish(this->postGreConnection);
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

int main(void)
{
	COutput coutput;

	std::cout << "  Opening connection..." << std::endl;
	std::cout << "    Output of connection: " << coutput.Initialise() << std::endl;

	std::cout << "  Creating Table..." << std::endl;
	std::cout << "    Table 'helloworld' created with output: "
			  << coutput.CreateTable("helloworld", "id serial PRIMARY KEY, name VARCHAR (50)")
			  << std::endl;

	std::cout << "  Inserting Row..." << std::endl;
	std::cout << "    Inserted two rows (12, 'Asterix') and (14, 'Idefix') with outputs: "
			  << coutput.InsertRow("helloworld(id, name)", "12, 'Asterix'") << ' '
			  << coutput.InsertRow("helloworld", "14, 'Idefix'")
			  << std::endl;

	std::cout << "  Closing connection..." << std::endl;
	coutput.Shutdown();
	std::cout << "  All done." << std::endl;

	return 0;
}

