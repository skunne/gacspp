#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>

#include "constants.h"
#include "CCloudGCP.hpp"
#include "COutput.hpp"
#include "CSimpleSim.hpp"

int main(int argc, char** argv)
{
    COutput& output = COutput::GetRef();

    {
        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::stringstream dbFileNamePath;
        dbFileNamePath << std::put_time(std::localtime(&now), "%H-%M-%S") << "-output.db";
        if(!output.Initialise(dbFileNamePath.str()))
            return 1;
    }

    TickType maxTick = 3600 * 24 * 30;
    if(argc>1)
    {
        maxTick = 3600 * 24 * static_cast<TickType>(std::stoul(argv[1]));
        std::cout<<"MaxTick="<<maxTick<<std::endl;
    }
    std::unique_ptr<CSimpleSim> sim(new CSimpleSim);
    sim->mClouds.emplace_back(new gcp::CCloud("GCP"));
    sim->SetupDefaults();

    output.StartConsumer();
    sim->Run(maxTick);
    output.Shutdown();

	int a;
	std::cin >> a;
}
