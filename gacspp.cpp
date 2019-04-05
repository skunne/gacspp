#include <iomanip>
#include <iostream>
#include <sstream>

#include "CAdvancedSim.hpp"
#include "COutput.hpp"
#include "CSimpleSim.hpp"

int main(int argc, char** argv)
{
    COutput& output = COutput::GetRef();

    {
        std::stringstream dbFileNamePath;
    #ifdef STATIC_DB_NAME
        dbFileNamePath << STATIC_DB_NAME;
    #else
        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        dbFileNamePath << std::put_time(std::localtime(&now), "%H-%M-%S") << "-output.db";
    #endif
        if(!output.Initialise(dbFileNamePath.str()))
            return 1;
    }

    TickType maxTick = 3600 * 24 * 30;
    if(argc>1)
    {
        maxTick = 3600 * 24 * static_cast<TickType>(std::stoul(argv[1]));
        std::cout<<"MaxTick="<<maxTick<<std::endl;
    }

    //auto sim = std::make_unique<CSimpleSim>();
    auto sim = std::make_unique<CAdvancedSim>();
    sim->SetupDefaults();

    output.StartConsumer();
    sim->Run(maxTick);
    output.Shutdown();

	int a;
	std::cin >> a;
}
