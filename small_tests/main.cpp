#include <iostream>
#include <algorithm>		/* std::count() */

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

/*
** Transform "Hello ? World ?" into "Hello $1 World $2"
*/
void COutput::replaceQuestionMarks(std::string& statementString)
{
    std::size_t n = 1;
    std::size_t index = statementString.find('?');
    while (index != std::string::npos)
    {
        std::string str = "$n";
        str.replace(1, 1, std::to_string(n));
        statementString.replace(index, 1, str);
        ++n;
        index = statementString.find('?');
    }
}

auto COutput::AddPreparedSQLStatement(const std::string& queryString) -> std::size_t
{
    /* // SQL
    sqlite3_stmt* preparedStatement;
    sqlite3_prepare_v3(mDB, statementString.c_str(), statementString.size() + 1, SQLITE_PREPARE_PERSISTENT, &preparedStatement, nullptr);
    assert(preparedStatement != nullptr);
    mPreparedStatements.emplace_back(preparedStatement);
    return (mPreparedStatements.size() - 1); */

    std::string statementString(queryString);   // local non-const copy
    std::string stmtName = std::to_string(nbPreparedStatements);
    int nParams = std::count(statementString.begin(), statementString.end(), '?');
    replaceQuestionMarks(statementString);
    const Oid *paramTypes = NULL;   // ???
    PQprepare(postGreConnection, stmtName.c_str(), statementString.c_str(), nParams, paramTypes);
    nbPreparedStatements += 1;
    return nbPreparedStatements;
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

	std::cout << "  Preparing 1 Statement..." << std::endl;
	std::cout << "    " << coutput.AddPreparedSQLStatement("SELECT ? FROM ?")
			  << " Done." << std::endl;

	std::cout << "  Closing connection..." << std::endl;
	coutput.Shutdown();
	std::cout << "  All done." << std::endl;

	return 0;
}

