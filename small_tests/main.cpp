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

int main(void)
{
	COutput coutput;

	std::cout << "  About to open connection..." << std::endl;
	std::cout << "    Output of connection: " << coutput.Initialise() << std::endl;
	std::cout << "  About to close connection..." << std::endl;
	coutput.Shutdown();
	std::cout << "  All done." << std::endl;

	return 0;
}

