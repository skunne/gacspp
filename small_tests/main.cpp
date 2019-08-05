#include <iostream>

#include <libpq-fe.h>       /* PostgreSQL */

#include "main.hpp"

bool COutput::Initialise(void)
{
    this->postGreConnection = PQconnectdb("user=admin password=changeme host=dbod-skunne-testing.cern.ch port=6601");
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

int main(void)
{
	COutput coutput;

	std::cout << "  Opening connection..." << std::endl;
	std::cout << "    Output of connection: " << coutput.Initialise() << std::endl;

	std::cout << "  Creating Table..." << std::endl;
	std::cout << "    Table created with output: "
			  << coutput.CreateTable("Hello World",    "id serial PRIMARY KEY, name VARCHAR (50)")
			  << std::endl;

	std::cout << "  Closing connection..." << std::endl;
	coutput.Shutdown();
	std::cout << "  All done." << std::endl;

	return 0;
}

